type AppHeaderProps = {
  title?: string;
  subtitle?: string;
};

export function AppHeader({
  title = "DataOps Assistant",
  subtitle = "Streaming ETL assistant with live pipeline steps.",
}: AppHeaderProps) {
  return (
    <header
      className="flex items-center justify-between py-4"
      aria-label="Application header"
    >
      <div>
        <h1 className="text-xl font-semibold">{title}</h1>
        <p className="text-xs text-zinc-500">{subtitle}</p>
      </div>
    </header>
  );
}
