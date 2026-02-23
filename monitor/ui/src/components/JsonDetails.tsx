type JsonDetailsProps = {
  title?: string;
  value: unknown;
};

export function JsonDetails({ title = "View JSON", value }: JsonDetailsProps) {
  let pretty = "{}";
  try {
    pretty = JSON.stringify(value ?? {}, null, 2);
  } catch {
    pretty = "{}";
  }

  return (
    <details className="json-details">
      <summary>{title}</summary>
      <pre>{pretty}</pre>
    </details>
  );
}
