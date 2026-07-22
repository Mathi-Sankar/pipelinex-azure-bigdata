export default function KpiCard({ value, label }) {
  return (
    <div className="rounded-xl border border-edge bg-panel px-5 py-4">
      <div className="text-2xl font-bold text-slate-100">{value}</div>
      <div className="mt-1 text-sm text-slate-400">{label}</div>
    </div>
  );
}
