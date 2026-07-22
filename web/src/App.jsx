import { useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { fetchJSON } from "./api";
import KpiCard from "./components/KpiCard";
import ChartCard from "./components/ChartCard";

const BRL = (n) => "R$ " + Number(n).toLocaleString("en-US", { maximumFractionDigits: 0 });
const NUM = (n) => Number(n).toLocaleString("en-US");

const STACK = ["Azure Databricks", "ADLS Gen2", "Delta Lake", "MongoDB Atlas", "FastAPI", "React", "CI/CD"];

const axis = { stroke: "#64748b", fontSize: 12 };
const gridColor = "#2a3852";
const tooltipStyle = {
  background: "#0b1120",
  border: "1px solid #2a3852",
  borderRadius: 8,
  color: "#e2e8f0",
};

export default function App() {
  const [kpis, setKpis] = useState(null);
  const [categories, setCategories] = useState([]);
  const [states, setStates] = useState([]);
  const [trend, setTrend] = useState([]);
  const [error, setError] = useState(null);

  useEffect(() => {
    Promise.all([
      fetchJSON("/api/kpis"),
      fetchJSON("/api/top-categories?limit=10"),
      fetchJSON("/api/state-revenue"),
      fetchJSON("/api/revenue-trend"),
    ])
      .then(([k, c, s, t]) => {
        setKpis(k);
        setCategories(c);
        setStates(s.slice(0, 10));
        setTrend(t);
      })
      .catch((e) => setError(e.message));
  }, []);

  const kpiCards = kpis
    ? [
        { value: BRL(kpis.total_revenue), label: "Total Revenue" },
        { value: NUM(kpis.total_orders), label: "Orders" },
        { value: NUM(kpis.total_customers), label: "Customers" },
        { value: NUM(kpis.total_products), label: "Products" },
        { value: kpis.avg_review_score.toFixed(2), label: "Avg Review Score" },
        { value: kpis.avg_delivery_days.toFixed(1) + " d", label: "Avg Delivery" },
      ]
    : [];

  return (
    <div className="mx-auto max-w-6xl px-6 py-8 text-slate-100">
      <header className="mb-8">
        <h1 className="text-3xl font-bold">PipelineX — Azure E-Commerce Analytics</h1>
        <p className="mt-2 max-w-3xl text-slate-400">
          A live dashboard served by a FastAPI backend over the Gold star-schema produced by an
          Azure Databricks + Delta Lake pipeline, with MongoDB Atlas enrichment. Built by Mathi
          Sankar M R.
        </p>
        <div className="mt-3 flex flex-wrap gap-2">
          {STACK.map((s) => (
            <span
              key={s}
              className="rounded-full border border-edge bg-panel2 px-3 py-1 text-xs text-sky"
            >
              {s}
            </span>
          ))}
        </div>
      </header>

      {error && (
        <div className="rounded-lg border border-red-500/40 bg-red-500/10 p-4 text-red-300">
          Failed to load data: {error}. If the backend is on Render's free tier, it may be waking
          up — refresh in ~30 seconds.
        </div>
      )}

      {/* KPI row */}
      <section className="mb-6 grid grid-cols-2 gap-4 sm:grid-cols-3 lg:grid-cols-6">
        {kpiCards.map((c) => (
          <KpiCard key={c.label} value={c.value} label={c.label} />
        ))}
      </section>

      {/* Charts */}
      <section className="grid grid-cols-1 gap-4 md:grid-cols-2">
        <ChartCard title="Top Categories by Revenue">
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={categories} layout="vertical" margin={{ left: 40 }}>
              <CartesianGrid stroke={gridColor} horizontal={false} />
              <XAxis type="number" {...axis} tickFormatter={(v) => `${v / 1000}k`} />
              <YAxis type="category" dataKey="category" width={110} {...axis} />
              <Tooltip contentStyle={tooltipStyle} formatter={(v) => BRL(v)} />
              <Bar dataKey="revenue" fill="#38bdf8" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Revenue by State">
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={states} layout="vertical" margin={{ left: 20 }}>
              <CartesianGrid stroke={gridColor} horizontal={false} />
              <XAxis type="number" {...axis} tickFormatter={(v) => `${v / 1e6}M`} />
              <YAxis type="category" dataKey="state" width={40} {...axis} />
              <Tooltip contentStyle={tooltipStyle} formatter={(v) => BRL(v)} />
              <Bar dataKey="revenue" fill="#22c55e" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Monthly Revenue Trend" full>
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={trend} margin={{ left: 10 }}>
              <CartesianGrid stroke={gridColor} />
              <XAxis dataKey="month" {...axis} />
              <YAxis {...axis} tickFormatter={(v) => `${v / 1e6}M`} />
              <Tooltip contentStyle={tooltipStyle} formatter={(v) => BRL(v)} />
              <Line
                type="monotone"
                dataKey="revenue"
                stroke="#38bdf8"
                strokeWidth={2}
                dot={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      </section>

      <footer className="mt-8 border-t border-edge pt-4 text-center text-sm text-slate-400">
        Data:{" "}
        <a
          className="text-sky hover:underline"
          href="https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"
          target="_blank"
          rel="noopener"
        >
          Olist Brazilian E-Commerce (Kaggle)
        </a>{" "}
        · Code:{" "}
        <a
          className="text-sky hover:underline"
          href="https://github.com/Mathi-Sankar/pipelinex-azure-bigdata"
          target="_blank"
          rel="noopener"
        >
          GitHub
        </a>{" "}
        · Pipeline runs on Microsoft Azure
      </footer>
    </div>
  );
}
