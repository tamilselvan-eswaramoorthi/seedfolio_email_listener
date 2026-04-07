## Cloudflare Tunnel: Server Setup Guide
1. Installation
Install the cloudflared agent on your Linux server (Ubuntu/Debian).

```Bash
curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb -o cloudflared.deb
sudo dpkg -i cloudflared.deb
```
2. Authentication
Link your server to your Cloudflare account.

```Bash
cloudflared tunnel login
```
Action: Click the link provided, log in, and select your domain (e.g., knotwealth.com). This generates a cert.pem file.

3. Create the Tunnel
Create the permanent connection.

```Bash
cloudflared tunnel create database-tunnel
```
Important: Copy the Tunnel ID (UUID) and the path to the JSON credentials file from the output.

4. Configure DNS
Tell Cloudflare to point your domain to this specific tunnel.

```Bash
cloudflared tunnel route dns database-tunnel database.knotwealth.com
```

5. Configuration File (config.yml)
Create a folder to store your settings and create the config file.

```Bash
sudo mkdir -p /etc/cloudflared
sudo nano /etc/cloudflared/config.yml
```

Paste this content (replace with your IDs):

```YAML
tunnel: <YOUR_TUNNEL_UUID>
credentials-file: /etc/cloudflared/<YOUR_TUNNEL_UUID>.json

ingress:
  - hostname: database.knotwealth.com
    service: tcp://localhost:3306
  - service: http_status:404
```

6. Move Credentials & Secure
Move the JSON file to the system folder so the service can access it.

```Bash
sudo cp ~/.cloudflared/<YOUR_TUNNEL_UUID>.json /etc/cloudflared/
```

7. Run as a Permanent Service
Enable the tunnel to start automatically on every boot.

```Bash
# Install the system service
sudo cloudflared service install

# Start and enable
sudo systemctl start cloudflared
sudo systemctl enable cloudflared
```