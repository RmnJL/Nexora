# Nexora systemd Quickstart

This guide keeps Nexora running continuously with automatic restart.

## Easy Run (recommended)

Use one command instead of manual unit/env editing.

Outside server:

```bash
cd /root/Nexora
sudo bash deploy/easy-run.sh server --zone t1.phonexpress.ir
```

Inside server:

```bash
cd /root/nexora
sudo bash deploy/easy-run.sh client \
  --zone t1.phonexpress.ir \
  --resolvers 185.49.84.2,91.92.209.5,91.92.209.194 \
  --listen-port 1443 \
  --target-host 127.0.0.1 \
  --target-port 8443 \
  --max-conns 192 \
  --max-conns-per-ip 64 \
  --poll-min-interval 0.12 \
  --poll-max-interval 1.2 \
  --idle-timeout 25
```

Status and logs:

```bash
sudo bash deploy/easy-run.sh status
sudo bash deploy/easy-run.sh logs client
sudo bash deploy/easy-run.sh logs server
```

## 1) Outside server (`nexora-server`)

Copy unit file and create env override:

```bash
sudo cp /root/Nexora/deploy/systemd/nexora-server.service /etc/systemd/system/
sudo tee /etc/default/nexora-server >/dev/null <<'EOF'
NEXORA_BIND=0.0.0.0
NEXORA_PORT=53
NEXORA_ZONE=t1.phonexpress.ir
NEXORA_PROTOCOL_VERSION=1
NEXORA_SESSION_TTL=900
NEXORA_CLEANUP_INTERVAL=60
EOF
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now nexora-server
sudo systemctl status nexora-server --no-pager
```

Logs:

```bash
sudo journalctl -u nexora-server -f
```

## 2) Inside server (`nexora-client-forward`)

Copy unit file and create env override:


```bash
sudo cp /root/nexora/deploy/systemd/nexora-client-forward.service /etc/systemd/system/
sudo tee /etc/default/nexora-client-forward >/dev/null <<'EOF'
NEXORA_RESOLVERS=185.49.84.2,91.92.209.5,91.92.209.194
NEXORA_PORT=53
NEXORA_ZONE=t1.phonexpress.ir
NEXORA_QTYPE=TXT
NEXORA_PROTOCOL_VERSION=1
NEXORA_TIMEOUT=3
NEXORA_ATTEMPTS=10
NEXORA_RESOLVER_FAIL_COOLDOWN=20
NEXORA_RESOLVER_HEALTH_INTERVAL=120
NEXORA_RESOLVER_SWITCH_INTERVAL=300
NEXORA_RESOLVER_PROBE_TIMEOUT=1.6
NEXORA_RESOLVER_PROBE_QTYPE=TXT
NEXORA_TCP_CHUNK_SIZE=24
NEXORA_FORWARD_LISTEN_HOST=0.0.0.0
NEXORA_FORWARD_LISTEN_PORT=1443
NEXORA_FORWARD_TARGET_HOST=127.0.0.1
NEXORA_FORWARD_TARGET_PORT=8443
NEXORA_FORWARD_MAX_CONNS=128
NEXORA_FORWARD_MAX_CONNS_PER_IP=64
NEXORA_STREAM_OPEN_RETRIES=5
NEXORA_FORWARD_POLL_MIN_INTERVAL=0.12
NEXORA_FORWARD_POLL_MAX_INTERVAL=1.2
NEXORA_FORWARD_IDLE_TIMEOUT=25
EOF
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now nexora-client-forward
sudo systemctl status nexora-client-forward --no-pager
```

Logs:

```bash
sudo journalctl -u nexora-client-forward -f
```

## 3) Update workflow

After code update:

```bash
cd /root/nexora && git pull origin main
sudo systemctl restart nexora-client-forward
```

and on outside:

```bash
cd /root/Nexora && git pull origin main
sudo systemctl restart nexora-server
```
