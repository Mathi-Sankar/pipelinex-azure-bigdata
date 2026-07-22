# PipelineX Dashboard (React)

A React + Vite + Tailwind single-page dashboard for the PipelineX Azure data
pipeline. Charts render with Recharts. It calls the FastAPI backend (deployed
on Render) and is itself deployed on Vercel.

## Stack
- **React 18** + **Vite 6** — component-based SPA with fast builds
- **Tailwind CSS** — utility-first styling (dark theme)
- **Recharts** — bar + line charts

## Run locally

```bash
cd web
npm install
npm run dev            # http://localhost:5173
```

The backend must be running at `http://localhost:8000` (see [`../api`](../api)),
or set `VITE_API_URL` to a deployed backend.

## Build

```bash
npm run build          # outputs to dist/
npm run preview        # serve the production build
```

## Deploy (Vercel)

1. Import the repo at [vercel.com](https://vercel.com)
2. Set **Root Directory** to `web`
3. Add environment variable `VITE_API_URL` = your Render backend URL
4. Deploy — Vercel auto-builds on every push
