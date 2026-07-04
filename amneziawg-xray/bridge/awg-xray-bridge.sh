#!/usr/bin/env bash
#
# awg-xray-bridge.sh
# ------------------
# Заворачивает трафик клиентов AmneziaWG (интерфейс awg0) во входящий
# Xray 'tun' (интерфейс xray0), чтобы им занималась маршрутизация/outbounds Xray.
#
# Идемпотентно. Вызывается systemd-юнитом awg-xray-bridge.service, который
# запускается по появлению интерфейса xray0 (awg-xray-bridge.path). При каждом
# перезапуске Xray интерфейс xray0 пересоздаётся -> маршрут в таблице RT_TABLE
# нужно проставить заново, что и делает этот скрипт.
#
# Схема пакета (клиент -> сайт):
#   awg0(10.9.9.x) --[ip rule from 10.9.9.0/24 -> table 100]--> default dev xray0
#     -> Xray tun читает пакет -> routing -> outbound (freedom/цепочка) -> eth0
#   Ответ: Xray пишет пакет (dst=10.9.9.x) в xray0 -> main table -> awg0 -> клиент
#
# Петля Xray-uplink исключена: собственные сокеты Xray имеют src=IP сервера
# (не из 10.9.9.0/24), поэтому НЕ попадают под ip rule и уходят через main/eth0.

set -euo pipefail

# --- Параметры (можно переопределить через окружение / /etc/awg-xray/bridge.env) ---
[ -f /etc/awg-xray/bridge.env ] && . /etc/awg-xray/bridge.env

AWG_IF="${AWG_IF:-awg0}"                 # интерфейс AmneziaWG
AWG_SUBNET="${AWG_SUBNET:-10.9.9.0/24}"  # подсеть клиентов AmneziaWG
TUN_IF="${TUN_IF:-xray0}"                # интерфейс Xray tun
TUN_ADDR="${TUN_ADDR:-172.16.250.1/30}"  # адрес на xray0 (техн., для валидного маршрута)
RT_TABLE="${RT_TABLE:-100}"              # номер таблицы маршрутизации
RULE_PRIO="${RULE_PRIO:-100}"            # приоритет ip rule

log() { echo "[awg-xray-bridge] $*"; }

if [ "$(id -u)" -ne 0 ]; then
  echo "Требуются права root." >&2
  exit 1
fi

# xray0 должен существовать (юнит .path это гарантирует, но проверим).
if ! ip link show "$TUN_IF" >/dev/null 2>&1; then
  log "Интерфейс $TUN_IF ещё не создан (Xray с tun-инбаундом не запущен?) — выходим."
  exit 0
fi

# 1) Адрес и состояние xray0. Xray создаёт интерфейс, но адрес мог не
#    примениться из 'gateway' — проставим сами (нужен для корректного маршрута).
ip link set dev "$TUN_IF" up 2>/dev/null || true
if ! ip addr show dev "$TUN_IF" | grep -q "${TUN_ADDR%/*}"; then
  ip addr add "$TUN_ADDR" dev "$TUN_IF" 2>/dev/null || true
fi

# 2) rp_filter=loose на самом xray0 (на случай, если all/default не покрыли).
sysctl -qw "net.ipv4.conf.${TUN_IF}.rp_filter=2" 2>/dev/null || true
sysctl -qw "net.ipv4.conf.${AWG_IF}.rp_filter=2" 2>/dev/null || true

# 3) ip rule: пакеты С подсети клиентов AmneziaWG -> таблица RT_TABLE.
if ! ip rule show | grep -q "from ${AWG_SUBNET} lookup ${RT_TABLE}"; then
  ip rule add from "$AWG_SUBNET" table "$RT_TABLE" priority "$RULE_PRIO"
  log "Добавлено ip rule: from ${AWG_SUBNET} -> table ${RT_TABLE}"
fi

# 4) Дефолтный маршрут таблицы RT_TABLE — в xray0 (весь трафик клиентов в Xray).
ip route replace default dev "$TUN_IF" table "$RT_TABLE"
log "Маршрут: default dev ${TUN_IF} table ${RT_TABLE}"

# 5) FORWARD: разрешаем пересылку awg0 <-> xray0 (на случай политики DROP).
ensure_fwd() { # $1=in $2=out
  iptables -C FORWARD -i "$1" -o "$2" -j ACCEPT 2>/dev/null \
    || iptables -I FORWARD 1 -i "$1" -o "$2" -j ACCEPT
}
ensure_fwd "$AWG_IF" "$TUN_IF"
ensure_fwd "$TUN_IF" "$AWG_IF"

# Разрешаем ответные/установленные соединения (если есть conntrack-политика).
if ! iptables -C FORWARD -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT 2>/dev/null; then
  iptables -I FORWARD 1 -m conntrack --ctstate RELATED,ESTABLISHED -j ACCEPT 2>/dev/null || true
fi

log "Мост awg0 -> xray0 настроен."
