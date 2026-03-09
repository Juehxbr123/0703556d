# Deploy notes (durak.clown-on-stonks.fun)

## 1) Environment
Copy `env.example` to `.env` (or export vars in systemd) and fill:
- `BOT_TOKEN`
- `TON_RECEIVER`
- `TONCENTER_API_KEY`
- `ADMIN_SECRET`
- `ADMIN_IDS`

Important:
- `PUBLIC_BASE_URL` must be exactly `https://durak.clown-on-stonks.fun`
- `API_PREFIX` must match frontend/backend (default `/api`)

## 2) Node service
Run app on `127.0.0.1:3000` (or `0.0.0.0:3000` with firewall).

Example systemd unit command:
```bash
npm ci --omit=dev
npm run start
```

## 3) Nginx
Install `deploy/nginx/durak.clown-on-stonks.fun.conf` into `/etc/nginx/sites-available/` and enable via symlink.

This config:
- redirects HTTP to HTTPS with **308**
- proxies **all** routes to Node (`/`, `/api/*`, `/ws`, `/tonconnect-manifest.json`)
- keeps websocket upgrade headers
- prevents static-fallback 405 on `POST /api/*`

Then:
```bash
nginx -t
systemctl reload nginx
```

## 4) Telegram bot polling
Server starts in polling mode and resets webhook if one is configured. Check logs after deploy for `[tg]` and `[pay]` entries.
