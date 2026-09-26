# FT-DOSS Cloud Deployment Guide

This guide details how to deploy the **FT-DOSS Distributed Object Storage System** to production cloud platforms.

---

## 1. Deploy Frontend to Vercel

### Option A: Vercel CLI (One Command)
```bash
cd frontend
npm i -g vercel
vercel --prod
```

### Option B: Vercel Web Dashboard (GitHub Integration)
1. Push this repository to GitHub: `https://github.com/phanendrababu464-lab/FT-DOSS`
2. Go to [Vercel Dashboard](https://vercel.com/new).
3. Import the `FT-DOSS` repository.
4. Set **Root Directory** to `frontend`.
5. Framework Preset: **Vite**.
6. Build Command: `npm run build`
7. Output Directory: `dist`
8. Click **Deploy**.

---

## 2. Deploy Frontend to Netlify

### Option A: Netlify CLI
```bash
npm i -g netlify-cli
netlify deploy --prod
```

### Option B: Netlify Web Interface
1. Log into [Netlify](https://app.netlify.com/).
2. Click **Add new site** → **Import an existing project**.
3. Select GitHub and choose `phanendrababu464-lab/FT-DOSS`.
4. Base directory: `frontend`
5. Build command: `npm run build`
6. Publish directory: `frontend/dist`
7. Click **Deploy FT-DOSS**.

---

## 3. Deploy Backend to Render

1. Go to [Render Dashboard](https://dashboard.render.com/).
2. Click **New +** → **Blueprint**.
3. Connect your GitHub repository `phanendrababu464-lab/FT-DOSS`.
4. Render will automatically read `render.yaml` and provision both:
   * **FT-DOSS API Gateway & Storage Backend** (Docker Container Service on port 8080)
   * **FT-DOSS Control Plane Dashboard** (Static Web Site)

---

## 4. Deploy Backend to Railway

1. Go to [Railway](https://railway.app/).
2. Click **New Project** → **Deploy from GitHub Repo**.
3. Select `phanendrababu464-lab/FT-DOSS`.
4. Add service from `backend/Dockerfile`.
5. Set Environment Variables:
   * `FTDOSS_NODE_ID`: `storage-1`
   * `FTDOSS_HTTP_PORT`: `8080`
   * `FTDOSS_ADMIN_API_KEY`: `dev-key`
6. Click **Deploy**.

---

## 5. Deploy Backend to Fly.io

```bash
cd backend
fly launch --config fly.toml
fly deploy
```

---

## 6. Full Stack Docker Compose Deployment

To deploy the entire multi-node cluster locally or on any cloud VPS (AWS EC2, DigitalOcean, Hetzner, GCP):

```bash
git clone https://github.com/phanendrababu464-lab/FT-DOSS.git
cd FT-DOSS
docker compose up -d --build
```

---

## Health Verification

After deployment, verify backend health:
```bash
curl https://your-backend-url.com/health
```

Output:
```json
{"service":"ft-doss-gateway","status":"ok"}
```
