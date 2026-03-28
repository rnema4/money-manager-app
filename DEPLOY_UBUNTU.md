# Ubuntu Stable Production Deployment

This guide gives you a stable production setup:
- `gunicorn` + `systemd` for app uptime
- optional persistent Cloudflare tunnel for public access from any network

If this Ubuntu is WSL, uptime still depends on your Windows machine staying on. For true 24x7 hosting, use a VPS Ubuntu server.

## 1) Install system packages

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip nginx mysql-server curl
```

## 2) Prepare app directory

```bash
sudo mkdir -p /opt/money_manager_app
sudo chown -R "$USER":"$USER" /opt/money_manager_app
```

Copy project files into:

`/opt/money_manager_app/money_manager_app`

## 3) Python env + dependencies

```bash
cd /opt/money_manager_app/money_manager_app
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements-prod.txt
```

## 4) MySQL setup

```bash
sudo mysql -e "CREATE DATABASE IF NOT EXISTS money_manager;"
sudo mysql -e "CREATE USER IF NOT EXISTS 'moneyapp'@'localhost' IDENTIFIED BY 'CHANGE_ME_STRONG_PASSWORD';"
sudo mysql -e "GRANT ALL PRIVILEGES ON money_manager.* TO 'moneyapp'@'localhost'; FLUSH PRIVILEGES;"
```

## 5) Production env file

```bash
cd /opt/money_manager_app/money_manager_app
cp deploy/.env.production.example .env.production
nano .env.production
```

Set:
- `DATABASE_URL` with your real DB password
- `SECRET_KEY` with a long random value

Generate a secret quickly:

```bash
python3 - << 'PY'
import secrets
print(secrets.token_urlsafe(48))
PY
```

## 6) Enable Gunicorn service

```bash
cd /opt/money_manager_app/money_manager_app
sudo cp deploy/money-manager.service /etc/systemd/system/money-manager.service
sudo sed -i "s/^User=.*/User=$USER/" /etc/systemd/system/money-manager.service
sudo systemctl daemon-reload
sudo systemctl enable --now money-manager
sudo systemctl status money-manager --no-pager
```

## 7) Public access option A (recommended without public IP): named Cloudflare tunnel

1. In Cloudflare Zero Trust dashboard, create a named tunnel.
2. Set public hostname to your domain and service URL to `http://127.0.0.1:8000`.
3. Copy the tunnel token.

Install `cloudflared`:

```bash
sudo curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o /usr/local/bin/cloudflared
sudo chmod +x /usr/local/bin/cloudflared
cloudflared --version
```

Create tunnel env:

```bash
cd /opt/money_manager_app/money_manager_app
cp deploy/.env.tunnel.example .env.tunnel
nano .env.tunnel
```

Paste token in `CLOUDFLARE_TUNNEL_TOKEN=...`

Enable tunnel service:

```bash
sudo cp deploy/money-manager-tunnel.service /etc/systemd/system/money-manager-tunnel.service
sudo sed -i "s/^User=.*/User=$USER/" /etc/systemd/system/money-manager-tunnel.service
sudo systemctl daemon-reload
sudo systemctl enable --now money-manager-tunnel
sudo systemctl status money-manager-tunnel --no-pager
```

## 8) Public access option B (public IP): Nginx reverse proxy

```bash
cd /opt/money_manager_app/money_manager_app
sudo cp deploy/nginx-money-manager.conf /etc/nginx/sites-available/money-manager
sudo nano /etc/nginx/sites-available/money-manager
```

Replace `YOUR_DOMAIN_OR_PUBLIC_IP`, then:

```bash
sudo ln -sf /etc/nginx/sites-available/money-manager /etc/nginx/sites-enabled/money-manager
sudo nginx -t
sudo systemctl restart nginx
sudo ufw allow OpenSSH
sudo ufw allow 'Nginx Full'
sudo ufw --force enable
```

HTTPS (domain only):

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d YOUR_DOMAIN
```

## 9) Health checks and logs

```bash
curl -I http://127.0.0.1:8000/login
sudo journalctl -u money-manager -f
sudo journalctl -u money-manager-tunnel -f
```

