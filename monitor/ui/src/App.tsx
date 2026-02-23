import { QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Navigate, Route, Routes, useLocation } from "react-router-dom";

import { AppShell } from "./components/AppShell";
import { queryClient } from "./lib/queryClient";
import { AlertsPage } from "./pages/AlertsPage";
import { EventsPage } from "./pages/EventsPage";
import { JobDetailPage } from "./pages/JobDetailPage";
import { JobsPage } from "./pages/JobsPage";
import { NotFoundPage } from "./pages/NotFoundPage";
import { OverviewPage } from "./pages/OverviewPage";

function AnimatedRoutes() {
  const location = useLocation();

  return (
    <div key={location.pathname} className="route-view">
      <Routes>
        <Route path="/" element={<OverviewPage />} />
        <Route path="/jobs" element={<JobsPage />} />
        <Route path="/jobs/:jobName" element={<JobDetailPage />} />
        <Route path="/alerts" element={<AlertsPage />} />
        <Route path="/events" element={<EventsPage />} />
        <Route path="/index.html" element={<Navigate to="/" replace />} />
        <Route path="*" element={<NotFoundPage />} />
      </Routes>
    </div>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AppShell>
          <AnimatedRoutes />
        </AppShell>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
