# Lead Email System - Deployment Guide

This guide covers the deployment of the Lead Email System backend and frontend on an Ubuntu server (e.g., AWS EC2, DigitalOcean, Hetzner) using Nginx, PM2, and SSL.

## 1. Prerequisites

Run the following commands on your Ubuntu server to install necessary software:

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install standard tools
sudo apt install -y curl git build-essential nginx

# Install Node.js 20.x
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs

# Install Redis (Required for Queues/BullMQ)
sudo apt install -y redis-server
sudo systemctl enable redis-server
sudo systemctl start redis-server

# Install PM2 (Process Manager)
sudo npm install -g pm2
```

## 2. PostgreSQL Database
Ensure you have a PostgreSQL database ready. You can install it locally on the server or use a managed service (Aiven, RDS, etc.).

**To install locally (optional):**
```bash
sudo apt install -y postgresql postgresql-contrib
sudo systemctl start postgresql
sudo -u postgres psql
# Inside psql:
# CREATE DATABASE lead_email_system;
# CREATE USER admin WITH ENCRYPTED PASSWORD 'your_password';
# GRANT ALL PRIVILEGES ON DATABASE lead_email_system TO admin;
# \q
```

## 3. Project Setup

Upload your code to the server (e.g., via `git clone` or SCP). We'll assume the app is located at `/var/www/leadflow`.

```bash
# Example setup
sudo mkdir -p /var/www/leadflow
sudo chown -R $USER:$USER /var/www/leadflow

# Navigate to server directory
cd /var/www/leadflow/server
```

### Install Dependencies
```bash
npm ci --production
# Install prisma globally for CLI tools if needed
sudo npm install -g prisma
```

### Environment Configuration
Create the production `.env` file:
```bash
cp .env.example .env
nano .env
```
**Critical Variables to Set:**
- `NODE_ENV=production`
- `PORT=3000`
- `DATABASE_URL`: Your PostgreSQL connection string.
- `REDIS_HOST`: `127.0.0.1` (if running locally).
- `BREVO_API_KEY`: Your email provider key.

## 4. Database Setup (CRITICAL STEP)
Before starting the application, you **MUST** apply the database schema changes and backfill the new data categories.

```bash
# 1. Generate Prisma Client
npx prisma generate

# 2. Push Schema to Database (adds 'category' column)
npx prisma db push --accept-data-loss

# 3. Backfill Data (Fixes missing categories for existing data)
# IMPORTANT: This script ensures your Analytics page works correctly
node scripts/backfill_categories.js
```

## 5. Start Backend with PM2
We use the `ecosystem.config.js` file (created in the server root) to manage the process.

```bash
# Start the app
pm2 start ecosystem.config.js

# Save configuration to auto-start on reboot
pm2 save
pm2 startup
# (Run the command output by pm2 startup)
```

## 6. Frontend Build (Optional)
If you are serving the frontend from the same server:

```bash
cd /var/www/leadflow/client
npm install
npm run build
# The build output is now in /var/www/leadflow/client/dist
```

## 7. Nginx Configuration
Setup Nginx to reverse proxy API requests to your Node app and serve the Frontend.

Create the config file: `sudo nano /etc/nginx/sites-available/leadflow`

```nginx
# /etc/nginx/sites-available/leadflow

upstream backend_api {
    server 127.0.0.1:3000;
}

server {
    listen 80;
    server_name yourdomain.com; # REPLACE THIS

    # Frontend (Static Files)
    location / {
        root /var/www/leadflow/client/dist;
        try_files $uri $uri/ /index.html;
    }

    # Backend API
    location /api/ {
        proxy_pass http://backend_api;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_cache_bypass $http_upgrade;
    }
    
    # Socket.IO
    location /socket.io/ {
        proxy_pass http://backend_api;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
    }
}
```

Enable the site:
```bash
sudo ln -s /etc/nginx/sites-available/leadflow /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

## 8. SSL with Certbot
Secure your site with HTTPS (free):

```bash
sudo apt install -y python3-certbot-nginx
sudo certbot --nginx -d yourdomain.com
```

## Verification
1. Check PM2 status: `pm2 status`
2. View Logs: `pm2 logs leadflow-backend`
3. Visit `https://yourdomain.com` and test the Login/Dashboard.

### Troubleshooting
- **Database Error?**: Check `DATABASE_URL` in `.env` and ensure `db push` ran successfully.
- **502 Bad Gateway?**: App might be crashed. Check `pm2 logs`.
- **Analytics Empty?**: Ensure `node scripts/backfill_categories.js` was run.
