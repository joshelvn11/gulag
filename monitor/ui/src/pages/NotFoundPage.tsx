import { Link } from "react-router-dom";

export function NotFoundPage() {
  return (
    <section className="stack">
      <div className="card">
        <h2>Page not found</h2>
        <p className="muted">The route you requested does not exist in the monitor dashboard.</p>
        <Link className="button button--primary" to="/">
          Go to Overview
        </Link>
      </div>
    </section>
  );
}
