#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "Run as root: sudo bash deploy/easy-run.sh ..."
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
SYSTEMD_SRC_DIR="${ROOT_DIR}/deploy/systemd"

SERVER_UNIT_SRC="${SYSTEMD_SRC_DIR}/nexora-server.service"
CLIENT_UNIT_SRC="${SYSTEMD_SRC_DIR}/nexora-client-forward.service"

SERVER_UNIT_DST="/etc/systemd/system/nexora-server.service"
CLIENT_UNIT_DST="/etc/systemd/system/nexora-client-forward.service"

SERVER_ENV="/etc/default/nexora-server"
CLIENT_ENV="/etc/default/nexora-client-forward"

usage() {
  cat <<'EOF'
Nexora Easy Run

Usage:
  sudo bash deploy/easy-run.sh server [options]
  sudo bash deploy/easy-run.sh client [options]
  sudo bash deploy/easy-run.sh status
  sudo bash deploy/easy-run.sh logs server|client
  sudo bash deploy/easy-run.sh restart server|client
  sudo bash deploy/easy-run.sh stop server|client

Server options:
  --bind IP                  default: 0.0.0.0
  --port PORT                default: 53
  --zone DOMAIN              default: t1.phonexpress.ir
  --session-ttl SECONDS      default: 900
  --cleanup-interval SECONDS default: 60

Client options:
  --resolvers CSV            default: 185.49.84.2,91.92.209.5,91.92.209.194
  --port PORT                default: 53
  --zone DOMAIN              default: t1.phonexpress.ir
  --qtype TXT|A              default: TXT
  --timeout SECONDS          default: 3
  --attempts N               default: 4
  --resolver-attempt-cap N   default: 6
  --resolver-fail-cooldown   default: 5
  --resolver-max-inflight N  default: 2
  --resolver-health-interval default: 90
  --resolver-switch-interval default: 180
  --resolver-probe-timeout   default: 1.4
  --resolver-probe-qtype     default: TXT
  --tcp-chunk-size N         default: 100
  --listen-host IP           default: 0.0.0.0
  --listen-port PORT         default: 1443
  --target-host IP           default: 127.0.0.1
  --target-port PORT         default: 8443
  --max-conns N              default: 24
  --max-conns-per-ip N       default: 64
  --stream-open-retries N    default: 2
  --dns-query-interval SEC   default: 0.04
  --poll-min-interval SEC    default: 0.12
  --poll-max-interval SEC    default: 3.0
  --idle-timeout SEC         default: 12
EOF
}

need_files() {
  if [[ ! -f "${SERVER_UNIT_SRC}" || ! -f "${CLIENT_UNIT_SRC}" ]]; then
    echo "Service templates not found under ${SYSTEMD_SRC_DIR}"
    exit 1
  fi
}

reload_and_enable() {
  local unit="$1"
  systemctl daemon-reload
  systemctl enable --now "${unit}"
  systemctl restart "${unit}"
  systemctl --no-pager --full status "${unit}" | sed -n '1,14p'
}

install_server() {
  local bind="0.0.0.0"
  local port="53"
  local zone="t1.phonexpress.ir"
  local session_ttl="900"
  local cleanup_interval="60"
  local server_py="/root/Nexora/src/nexora_server.py"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --bind) bind="${2:?}"; shift 2 ;;
      --port) port="${2:?}"; shift 2 ;;
      --zone) zone="${2:?}"; shift 2 ;;
      --session-ttl) session_ttl="${2:?}"; shift 2 ;;
      --cleanup-interval) cleanup_interval="${2:?}"; shift 2 ;;
      *) echo "Unknown option for server: $1"; usage; exit 1 ;;
    esac
  done

  if [[ -f "/root/Nexora/src/nexora_server.py" ]]; then
    server_py="/root/Nexora/src/nexora_server.py"
  elif [[ -f "/root/nexora/src/nexora_server.py" ]]; then
    server_py="/root/nexora/src/nexora_server.py"
  fi

  cp "${SERVER_UNIT_SRC}" "${SERVER_UNIT_DST}"
  cat > "${SERVER_ENV}" <<EOF
NEXORA_SERVER_PY=${server_py}
NEXORA_BIND=${bind}
NEXORA_PORT=${port}
NEXORA_ZONE=${zone}
NEXORA_SESSION_TTL=${session_ttl}
NEXORA_CLEANUP_INTERVAL=${cleanup_interval}
EOF
  echo "[easy-run] installed ${SERVER_UNIT_DST}"
  echo "[easy-run] wrote ${SERVER_ENV}"
  reload_and_enable "nexora-server"
}

install_client() {
  local resolvers="185.49.84.2,91.92.209.5,91.92.209.194"
  local port="53"
  local zone="t1.phonexpress.ir"
  local qtype="TXT"
  local timeout="2.5"
  local attempts="4"
  local resolver_attempt_cap="6"
  local resolver_fail_cooldown="5"
  local resolver_max_inflight="2"
  local resolver_health_interval="90"
  local resolver_switch_interval="180"
  local resolver_probe_timeout="1.4"
  local resolver_probe_qtype="TXT"
  local tcp_chunk_size="100"
  local listen_host="0.0.0.0"
  local listen_port="1443"
  local target_host="127.0.0.1"
  local target_port="8443"
  local max_conns="24"
  local max_conns_per_ip="64"
  local stream_open_retries="2"
  local dns_query_interval="0.04"
  local poll_min_interval="0.12"
  local poll_max_interval="3.0"
  local idle_timeout="12"
  local client_py="/root/nexora/src/nexora_client.py"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --resolvers) resolvers="${2:?}"; shift 2 ;;
      --port) port="${2:?}"; shift 2 ;;
      --zone) zone="${2:?}"; shift 2 ;;
      --qtype) qtype="${2:?}"; shift 2 ;;
      --timeout) timeout="${2:?}"; shift 2 ;;
      --attempts) attempts="${2:?}"; shift 2 ;;
      --resolver-attempt-cap) resolver_attempt_cap="${2:?}"; shift 2 ;;
      --resolver-fail-cooldown) resolver_fail_cooldown="${2:?}"; shift 2 ;;
      --resolver-max-inflight) resolver_max_inflight="${2:?}"; shift 2 ;;
      --resolver-health-interval) resolver_health_interval="${2:?}"; shift 2 ;;
      --resolver-switch-interval) resolver_switch_interval="${2:?}"; shift 2 ;;
      --resolver-probe-timeout) resolver_probe_timeout="${2:?}"; shift 2 ;;
      --resolver-probe-qtype) resolver_probe_qtype="${2:?}"; shift 2 ;;
      --tcp-chunk-size) tcp_chunk_size="${2:?}"; shift 2 ;;
      --listen-host) listen_host="${2:?}"; shift 2 ;;
      --listen-port) listen_port="${2:?}"; shift 2 ;;
      --target-host) target_host="${2:?}"; shift 2 ;;
      --target-port) target_port="${2:?}"; shift 2 ;;
      --max-conns) max_conns="${2:?}"; shift 2 ;;
      --max-conns-per-ip) max_conns_per_ip="${2:?}"; shift 2 ;;
      --stream-open-retries) stream_open_retries="${2:?}"; shift 2 ;;
      --dns-query-interval) dns_query_interval="${2:?}"; shift 2 ;;
      --poll-min-interval) poll_min_interval="${2:?}"; shift 2 ;;
      --poll-max-interval) poll_max_interval="${2:?}"; shift 2 ;;
      --idle-timeout) idle_timeout="${2:?}"; shift 2 ;;
      *) echo "Unknown option for client: $1"; usage; exit 1 ;;
    esac
  done

  if [[ -f "/root/nexora/src/nexora_client.py" ]]; then
    client_py="/root/nexora/src/nexora_client.py"
  elif [[ -f "/root/nexora/nexora_client.py" ]]; then
    client_py="/root/nexora/nexora_client.py"
  fi

  cp "${CLIENT_UNIT_SRC}" "${CLIENT_UNIT_DST}"
  cat > "${CLIENT_ENV}" <<EOF
NEXORA_CLIENT_PY=${client_py}
NEXORA_RESOLVERS=${resolvers}
NEXORA_PORT=${port}
NEXORA_ZONE=${zone}
NEXORA_QTYPE=${qtype}
NEXORA_TIMEOUT=${timeout}
NEXORA_ATTEMPTS=${attempts}
NEXORA_RESOLVER_ATTEMPT_CAP=${resolver_attempt_cap}
NEXORA_RESOLVER_FAIL_COOLDOWN=${resolver_fail_cooldown}
NEXORA_RESOLVER_MAX_INFLIGHT=${resolver_max_inflight}
NEXORA_RESOLVER_HEALTH_INTERVAL=${resolver_health_interval}
NEXORA_RESOLVER_SWITCH_INTERVAL=${resolver_switch_interval}
NEXORA_RESOLVER_PROBE_TIMEOUT=${resolver_probe_timeout}
NEXORA_RESOLVER_PROBE_QTYPE=${resolver_probe_qtype}
NEXORA_TCP_CHUNK_SIZE=${tcp_chunk_size}
NEXORA_FORWARD_LISTEN_HOST=${listen_host}
NEXORA_FORWARD_LISTEN_PORT=${listen_port}
NEXORA_FORWARD_TARGET_HOST=${target_host}
NEXORA_FORWARD_TARGET_PORT=${target_port}
NEXORA_FORWARD_MAX_CONNS=${max_conns}
NEXORA_FORWARD_MAX_CONNS_PER_IP=${max_conns_per_ip}
NEXORA_STREAM_OPEN_RETRIES=${stream_open_retries}
NEXORA_DNS_QUERY_INTERVAL=${dns_query_interval}
NEXORA_FORWARD_POLL_MIN_INTERVAL=${poll_min_interval}
NEXORA_FORWARD_POLL_MAX_INTERVAL=${poll_max_interval}
NEXORA_FORWARD_IDLE_TIMEOUT=${idle_timeout}
EOF
  echo "[easy-run] installed ${CLIENT_UNIT_DST}"
  echo "[easy-run] wrote ${CLIENT_ENV}"
  reload_and_enable "nexora-client-forward"
}

show_status() {
  if systemctl list-unit-files --type=service | grep -q '^nexora-server\.service'; then
    systemctl --no-pager --full status nexora-server || true
  else
    echo "[easy-run] nexora-server.service is not installed on this host."
  fi
  echo
  if systemctl list-unit-files --type=service | grep -q '^nexora-client-forward\.service'; then
    systemctl --no-pager --full status nexora-client-forward || true
  else
    echo "[easy-run] nexora-client-forward.service is not installed on this host."
  fi
}

show_logs() {
  local which="${1:-}"
  case "${which}" in
    server) journalctl -u nexora-server -f ;;
    client) journalctl -u nexora-client-forward -f ;;
    *) echo "Use logs server|client"; exit 1 ;;
  esac
}

service_action() {
  local action="$1"
  local which="${2:-}"
  local unit=""
  case "${which}" in
    server) unit="nexora-server" ;;
    client) unit="nexora-client-forward" ;;
    *) echo "Use ${action} server|client"; exit 1 ;;
  esac
  systemctl "${action}" "${unit}"
  systemctl --no-pager --full status "${unit}" | sed -n '1,12p'
}

main() {
  need_files
  local cmd="${1:-}"
  shift || true
  case "${cmd}" in
    server) install_server "$@" ;;
    client) install_client "$@" ;;
    status) show_status ;;
    logs) show_logs "$@" ;;
    restart) service_action restart "$@" ;;
    stop) service_action stop "$@" ;;
    -h|--help|"") usage ;;
    *) echo "Unknown command: ${cmd}"; usage; exit 1 ;;
  esac
}

main "$@"
