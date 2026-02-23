import express, { Request, Response } from "express";
import { z } from "zod";

import { loadConfig } from "./config.js";
import { createDbClient } from "./db/client.js";
import { runMigrations } from "./db/migrate.js";
import { MonitorService } from "./services/monitorService.js";

const eventSchema = z.object({
  sourceType: z.enum(["chief", "worker", "monitor"]),
  eventType: z.string().min(1),
  level: z.enum(["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"]),
  message: z.string().min(1),
  eventAt: z.string().optional(),
  jobName: z.string().optional(),
  scriptPath: z.string().optional(),
  runId: z.string().optional(),
  scheduledFor: z.string().optional(),
  success: z.boolean().optional(),
  returnCode: z.number().int().optional(),
  durationMs: z.number().int().optional(),
  metadata: z.record(z.unknown()).optional(),
});

const eventBatchSchema = z.union([
  z.array(eventSchema),
  z.object({ events: z.array(eventSchema) }),
]);

function coercePositiveInt(raw: unknown, fallback: number, max: number): number {
  if (typeof raw !== "string") {
    return fallback;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.min(parsed, max);
}

function authenticate(req: Request, configuredApiKey: string): boolean {
  if (!configuredApiKey) {
    return true;
  }
  const header = req.header("x-api-key") ?? "";
  return header === configuredApiKey;
}

function main(): void {
  const config = loadConfig();
  runMigrations(config.dbPath);

  const { db, sqlite } = createDbClient(config.dbPath);
  const service = new MonitorService(db, config);

  const app = express();
  app.use(express.json({ limit: "2mb" }));

  app.get("/v1/health", (_req: Request, res: Response) => {
    res.json({
      ok: true,
      service: "chief-monitor",
      now: new Date().toISOString(),
      dbPath: config.dbPath,
    });
  });

  app.post("/v1/events", (req: Request, res: Response) => {
    if (!authenticate(req, config.apiKey)) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const parsed = eventSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: "Invalid event payload", details: parsed.error.flatten() });
    }

    const result = service.ingestEvents([parsed.data]);
    return res.status(202).json(result);
  });

  app.post("/v1/events/batch", (req: Request, res: Response) => {
    if (!authenticate(req, config.apiKey)) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const parsed = eventBatchSchema.safeParse(req.body);
    if (!parsed.success) {
      return res.status(400).json({ error: "Invalid batch payload", details: parsed.error.flatten() });
    }

    const events = Array.isArray(parsed.data) ? parsed.data : parsed.data.events;
    const result = service.ingestEvents(events);
    return res.status(202).json(result);
  });

  app.get("/v1/status/summary", (_req: Request, res: Response) => {
    res.json(service.getSummary());
  });

  app.get("/v1/status/jobs", (_req: Request, res: Response) => {
    res.json({ jobs: service.getJobsStatus() });
  });

  app.get("/v1/status/jobs/:jobName", (req: Request, res: Response) => {
    const details = service.getJobDetails(req.params.jobName);
    if (!details.check) {
      return res.status(404).json({ error: "Job not found in check state", jobName: req.params.jobName });
    }
    return res.json(details);
  });

  app.get("/v1/alerts", (req: Request, res: Response) => {
    const limit = coercePositiveInt(req.query.limit, 100, 1000);
    const offset = coercePositiveInt(req.query.offset, 0, 1_000_000);

    const rows = service.listAlerts({
      jobName: typeof req.query.jobName === "string" ? req.query.jobName : undefined,
      status: typeof req.query.status === "string" ? req.query.status : undefined,
      alertType: typeof req.query.alertType === "string" ? req.query.alertType : undefined,
      severity: typeof req.query.severity === "string" ? req.query.severity : undefined,
      limit,
      offset,
    });

    res.json({ alerts: rows, limit, offset });
  });

  app.get("/v1/events", (req: Request, res: Response) => {
    const limit = coercePositiveInt(req.query.limit, 100, 1000);
    const offset = coercePositiveInt(req.query.offset, 0, 1_000_000);

    const rows = service.listEvents({
      jobName: typeof req.query.jobName === "string" ? req.query.jobName : undefined,
      scriptPath: typeof req.query.scriptPath === "string" ? req.query.scriptPath : undefined,
      level: typeof req.query.level === "string" ? req.query.level : undefined,
      eventType: typeof req.query.eventType === "string" ? req.query.eventType : undefined,
      from: typeof req.query.from === "string" ? req.query.from : undefined,
      to: typeof req.query.to === "string" ? req.query.to : undefined,
      limit,
      offset,
    });

    res.json({ events: rows, limit, offset });
  });

  const loops = service.startBackgroundLoops();

  const server = app.listen(config.port, config.host, () => {
    // eslint-disable-next-line no-console
    console.log(`chief-monitor listening on http://${config.host}:${config.port}`);
  });

  const shutdown = () => {
    for (const handle of loops) {
      clearInterval(handle);
    }
    server.close(() => {
      sqlite.close();
      process.exit(0);
    });
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main();
