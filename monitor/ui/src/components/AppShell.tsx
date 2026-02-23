import { NavLink } from "react-router-dom";
import type { PropsWithChildren } from "react";

const navItems = [
  { to: "/", label: "Overview" },
  { to: "/jobs", label: "Jobs" },
  { to: "/alerts", label: "Alerts" },
  { to: "/events", label: "Events" },
];

export function AppShell({ children }: PropsWithChildren) {
  return (
    <div className="shell">
      <header className="shell__header">
        <div className="brand">
          <span className="brand__dot" aria-hidden="true" />
          <div>
            <h1>Chief Monitor</h1>
            <p>Operational telemetry dashboard</p>
          </div>
        </div>
        <nav className="nav" aria-label="Primary">
          {navItems.map((item) => (
            <NavLink
              key={item.to}
              to={item.to}
              className={({ isActive }) => `nav__link${isActive ? " is-active" : ""}`}
              end={item.to === "/"}
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
      </header>
      <main className="shell__content">{children}</main>
    </div>
  );
}
