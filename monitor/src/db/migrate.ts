import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { loadConfig } from "../config.js";
import { createDbClient } from "./client.js";

export function runMigrations(dbPath: string): void {
  const { sqlite } = createDbClient(dbPath);
  const thisFile = fileURLToPath(import.meta.url);
  const root = path.resolve(path.dirname(thisFile), "../../");
  const migrationPath = path.join(root, "drizzle", "0000_init.sql");
  const sql = fs.readFileSync(migrationPath, "utf-8");
  sqlite.exec(sql);
  sqlite.close();
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const config = loadConfig();
  runMigrations(config.dbPath);
  // eslint-disable-next-line no-console
  console.log(`Applied monitor migrations to ${config.dbPath}`);
}
