export default function ChartCard({ title, children, full = false }) {
  return (
    <div
      className={`rounded-xl border border-edge bg-panel p-5 ${
        full ? "md:col-span-2" : ""
      }`}
    >
      <h2 className="mb-4 text-base font-semibold text-slate-100">{title}</h2>
      {children}
    </div>
  );
}
