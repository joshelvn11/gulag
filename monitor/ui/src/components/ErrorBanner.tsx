type ErrorBannerProps = {
  title?: string;
  message: string;
};

export function ErrorBanner({ title = "Request failed", message }: ErrorBannerProps) {
  return (
    <div className="error-banner" role="alert">
      <strong>{title}</strong>
      <p>{message}</p>
    </div>
  );
}
