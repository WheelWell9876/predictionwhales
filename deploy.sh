##!/bin/bash
#
## =========================================
## Polymarket Whale Tracker Deployment Script
## For Digital Ocean Droplet (Ubuntu 22.04)
## =========================================
#
#echo "================================================"
#echo "🐋 Polymarket Whale Tracker - Server Setup"
#echo "================================================"
#
## Update system
#echo -e "\n📦 Updating system packages..."
#sudo apt-get update
#sudo apt-get upgrade -y
#
## Install Python 3.10 and dependencies
#echo -e "\n🐍 Installing Python and dependencies..."
#sudo apt-get install -y python3.10 python3.10-venv python3-pip
#sudo apt-get install -y git nginx supervisor certbot python3-certbot-nginx
#sudo apt-get install -y build-essential libssl-dev libffi-dev python3-dev
#
## Create application directory
#echo -e "\n📁 Setting up application directory..."
#APP_DIR="/opt/polymarket-tracker"
#sudo mkdir -p $APP_DIR
#sudo chown $USER:$USER $APP_DIR
#cd $APP_DIR
#
## Clone or copy application files
#echo -e "\n📥 Setting up application files..."
## If you have a git repo:
## git clone https://github.com/yourusername/polymarket-tracker.git .
#
## Or create directories manually:
#mkdir -p services models static/css static/js
#
## Create virtual environment
#echo -e "\n🔧 Creating Python virtual environment..."
#python3.10 -m venv venv
#source venv/bin/activate
#
## Install Python packages
#echo -e "\n📚 Installing Python packages..."
#pip install --upgrade pip
#pip install fastapi uvicorn[standard] aiohttp web3 eth-account python-dotenv
#pip install pydantic pydantic-settings supervisor
#
## Create systemd service file
#echo -e "\n⚙️ Creating systemd service..."
#sudo tee /etc/systemd/system/polymarket-tracker.service > /dev/null <<EOF
#[Unit]
#Description=Polymarket Whale Tracker
#After=network.target
#
#[Service]
#Type=simple
#User=$USER
#WorkingDirectory=$APP_DIR
#Environment="PATH=$APP_DIR/venv/bin"
#ExecStart=$APP_DIR/venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2
#Restart=always
#RestartSec=10
#
#[Install]
#WantedBy=multi-user.target
#EOF
#
## Create Nginx configuration
#echo -e "\n🌐 Setting up Nginx..."
#sudo tee /etc/nginx/sites-available/polymarket-tracker > /dev/null <<'EOF'
#server {
#    listen 80;
#    server_name _;
#
#    client_max_body_size 10M;
#
#    location / {
#        proxy_pass http://127.0.0.1:8000;
#        proxy_http_version 1.1;
#        proxy_set_header Upgrade $http_upgrade;
#        proxy_set_header Connection 'upgrade';
#        proxy_set_header Host $host;
#        proxy_cache_bypass $http_upgrade;
#        proxy_set_header X-Real-IP $remote_addr;
#        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
#        proxy_set_header X-Forwarded-Proto $scheme;
#        proxy_connect_timeout 60s;
#        proxy_send_timeout 60s;
#        proxy_read_timeout 60s;
#    }
#
#    location /static {
#        alias /opt/polymarket-tracker/static;
#        expires 30d;
#        add_header Cache-Control "public, immutable";
#    }
#}
#EOF
#
## Enable Nginx site
#sudo ln -sf /etc/nginx/sites-available/polymarket-tracker /etc/nginx/sites-enabled/
#sudo rm -f /etc/nginx/sites-enabled/default
#sudo nginx -t
#sudo systemctl restart nginx
#
## Create log directory
#echo -e "\n📝 Setting up logging..."
#sudo mkdir -p /var/log/polymarket-tracker
#sudo chown $USER:$USER /var/log/polymarket-tracker
#
## Create supervisor configuration for background tasks
#echo -e "\n👀 Setting up Supervisor for background tasks..."
#sudo tee /etc/supervisor/conf.d/polymarket-tracker.conf > /dev/null <<EOF
#[program:polymarket-tracker]
#command=$APP_DIR/venv/bin/uvicorn main:app --host 127.0.0.1 --port 8000
#directory=$APP_DIR
#user=$USER
#autostart=true
#autorestart=true
#redirect_stderr=true
#stdout_logfile=/var/log/polymarket-tracker/app.log
#environment=PATH="$APP_DIR/venv/bin",PYTHONPATH="$APP_DIR"
#EOF
#
## Setup firewall
#echo -e "\n🔒 Configuring firewall..."
#sudo ufw allow OpenSSH
#sudo ufw allow 'Nginx Full'
#sudo ufw --force enable
#
## Create update script
#echo -e "\n📜 Creating update script..."
#cat > $APP_DIR/update.sh <<'EOF'
##!/bin/bash
#cd /opt/polymarket-tracker
#source venv/bin/activate
#git pull
#pip install -r requirements.txt
#sudo systemctl restart polymarket-tracker
#sudo systemctl restart nginx
#echo "✅ Update complete!"
#EOF
#chmod +x $APP_DIR/update.sh
#
## Create monitoring script
#echo -e "\n📊 Creating monitoring script..."
#cat > $APP_DIR/monitor.sh <<'EOF'
##!/bin/bash
#echo "🐋 Polymarket Tracker Status"
#echo "============================="
#echo ""
#echo "Service Status:"
#sudo systemctl status polymarket-tracker --no-pager
#echo ""
#echo "Recent Logs:"
#sudo journalctl -u polymarket-tracker -n 20 --no-pager
#echo ""
#echo "Disk Usage:"
#df -h /
#echo ""
#echo "Memory Usage:"
#free -h
#echo ""
#echo "Python Processes:"
#ps aux | grep python
#EOF
#chmod +x $APP_DIR/monitor.sh
#
## Create backup script
#echo -e "\n💾 Creating backup script..."
#cat > $APP_DIR/backup.sh <<'EOF'
##!/bin/bash
#BACKUP_DIR="/home/$USER/backups"
#mkdir -p $BACKUP_DIR
#DATE=$(date +%Y%m%d_%H%M%S)
#cd /opt/polymarket-tracker
#tar -czf $BACKUP_DIR/polymarket_backup_$DATE.tar.gz .env wallet_backup.json *.db
#echo "✅ Backup saved to $BACKUP_DIR/polymarket_backup_$DATE.tar.gz"
## Keep only last 7 backups
#ls -t $BACKUP_DIR/polymarket_backup_*.tar.gz | tail -n +8 | xargs rm -f
#EOF
#chmod +x $APP_DIR/backup.sh
#
## Add cron job for automatic backups
#echo -e "\n⏰ Setting up automatic backups..."
#(crontab -l 2>/dev/null; echo "0 2 * * * $APP_DIR/backup.sh") | crontab -
#
## Create .env template
#echo -e "\n📝 Creating .env template..."
#cat > $APP_DIR/.env.template <<'EOF'
## Polygon Wallet Configuration
#OBSERVER_PRIVATE_KEY=your_private_key_here
#OBSERVER_ADDRESS=your_wallet_address_here
#POLYGON_RPC_URL=https://polygon-rpc.com
#
## Polymarket Configuration
#MIN_BET_AMOUNT=1000
#MIN_WHALE_VOLUME=10000
#UPDATE_INTERVAL=300
#MAX_TRACKED_WALLETS=100
#MAX_CONCURRENT_REQUESTS=10
#
## Optional: Polymarket API Credentials (will be auto-generated)
## POLYMARKET_API_KEY=
## POLYMARKET_API_SECRET=
## POLYMARKET_API_PASSPHRASE=
#EOF
#
#echo "================================================"
#echo "✅ Server Setup Complete!"
#echo "================================================"
#echo ""
#echo "📋 Next Steps:"
#echo ""
#echo "1. Copy your application files to $APP_DIR"
#echo "   - main.py"
#echo "   - config.py"
#echo "   - services/*.py"
#echo "   - models/*.py"
#echo "   - static/*"
#echo ""
#echo "2. Set up your wallet:"
#echo "   cd $APP_DIR"
#echo "   source venv/bin/activate"
#echo "   python setup_wallet.py"
#echo ""
#echo "3. Start the service:"
#echo "   sudo systemctl daemon-reload"
#echo "   sudo systemctl enable polymarket-tracker"
#echo "   sudo systemctl start polymarket-tracker"
#echo ""
#echo "4. Check status:"
#echo "   sudo systemctl status polymarket-tracker"
#echo "   sudo journalctl -u polymarket-tracker -f"
#echo ""
#echo "5. Access your tracker:"
#echo "   http://your-server-ip"
#echo ""
#echo "📝 Useful Commands:"
#echo "   ./monitor.sh     - Check system status"
#echo "   ./update.sh      - Update application"
#echo "   ./backup.sh      - Create backup"
#echo ""
#echo "🔐 Security Notes:"
#echo "   - Change SSH port in /etc/ssh/sshd_config"
#echo "   - Set up SSL with: sudo certbot --nginx"
#echo "   - Use strong passwords"
#echo "   - Keep private keys secure"
#echo ""