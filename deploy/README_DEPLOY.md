# Deploy (durak.clown-on-stonks.fun)

## 1) Nginx config install
```bash
sudo cp deploy/nginx/durak.clown-on-stonks.fun.conf /etc/nginx/sites-available/durak.clown-on-stonks.fun.conf
sudo ln -sf /etc/nginx/sites-available/durak.clown-on-stonks.fun.conf /etc/nginx/sites-enabled/durak.clown-on-stonks.fun.conf
sudo nginx -t
sudo systemctl reload nginx
```

## 2) Node app restart (PM2)
```bash
pm2 restart durak-mini-app --update-env
```

If your process has another name:
```bash
pm2 list
pm2 restart <your-process-name> --update-env
```

## 3) Required env
- `PUBLIC_BASE_URL=https://durak.clown-on-stonks.fun`
- `BOT_TOKEN=...`
- `TON_RECEIVER=...`
- `TONCENTER_API_KEY=...`
- `ADMIN_SECRET=...`
- `ADMIN_IDS=123,456`
- `API_PREFIX=/api`
