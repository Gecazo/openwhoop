# OpenWhoop Web

This folder is reserved for the web frontend.

The initial product direction is:

- Python backend in `apps/api`
- Web dashboard frontend in `apps/web`
- The existing Rust sync tool continues to pull data from the WHOOP device
- The web app reads dashboard data from the Python API

Suggested next step:

1. Scaffold a Next.js app here
2. Build a dashboard page that consumes `/api/v1/dashboard`
3. Add chart views using `/api/v1/heart-rate`

For now there is also a tiny static prototype in `index.html` that can be opened directly in a browser while the Python API is running locally.
