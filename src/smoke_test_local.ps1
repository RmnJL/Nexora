# Nexora local smoke test (phase 1)
# Signature: Rmn JL

param(
  [string]$Zone = "t1.phonexpress.ir",
  [int]$Port = 5300
)

$server = Start-Process python -ArgumentList "nexora_server.py --bind 127.0.0.1 --port $Port --zone $Zone" -PassThru -WindowStyle Hidden
Start-Sleep -Seconds 1

try {
  python nexora_client.py --server 127.0.0.1 --port $Port --zone $Zone --timeout 2
}
finally {
  Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
}

