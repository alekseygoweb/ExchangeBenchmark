#!/usr/bin/env bash
#
# install-amneziawg.sh
# --------------------
# Ставит AmneziaWG 2.0 и создаёт серверный конфиг awg0, ЗАТОЧЕННЫЙ под передачу
# трафика в Xray (входящий tun xray0). Ключевое отличие от обычного WG-сервера:
# в конфиге НЕТ MASQUERADE/NAT — трафик клиентов не выпускается напрямую, а
# заворачивается в xray0 (это делает setup-bridge.sh + awg-xray-bridge.sh).
#
# Поддержка установки бинарников:
#   * Ubuntu           -> официальный PPA ppa:amnezia/ppa (модуль ядра DKMS + tools)
#   * Debian / прочее  -> подсказка (используйте bivlked/amneziawg-installer или
#                          соберите из исходников), затем --skip-install для конфига.
#
# Флаги:
#   --skip-install   не ставить бинарники (awg уже установлен другим способом),
#                    только сгенерировать конфиг и включить сервис.
#   --port N         UDP-порт AmneziaWG (по умолчанию 39743).
#   --endpoint HOST  публичный адрес сервера для клиентских конфигов (по умолчанию
#                    автоопределение).
#
set -euo pipefail

AWG_PORT=39743
AWG_ENDPOINT=""
SKIP_INSTALL=0

while [ $# -gt 0 ]; do
  case "$1" in
    --skip-install) SKIP_INSTALL=1; shift ;;
    --port) AWG_PORT="$2"; shift 2 ;;
    --endpoint) AWG_ENDPOINT="$2"; shift 2 ;;
    *) echo "Неизвестный аргумент: $1" >&2; exit 1 ;;
  esac
done

[ "$(id -u)" -eq 0 ] || { echo "Запустите от root (sudo)." >&2; exit 1; }

# --- Константы схемы (должны совпадать с bridge/*.sh) ---
AWG_IF="awg0"
AWG_SUBNET="10.9.9.0/24"
AWG_SERVER_IP="10.9.9.1"
AWG_CONF_DIR="/etc/amnezia/amneziawg"
AWG_CONF="${AWG_CONF_DIR}/${AWG_IF}.conf"
ENV_DIR="/etc/awg-xray"
ENV_FILE="${ENV_DIR}/bridge.env"

log() { echo -e "\n\033[1;36m==>\033[0m $*"; }

# ---------------------------------------------------------------------------
# 1) Установка бинарников AmneziaWG
# ---------------------------------------------------------------------------
install_binaries() {
  if command -v awg >/dev/null 2>&1; then
    log "awg уже установлен ($(awg --version 2>/dev/null | head -n1)) — пропускаю установку."
    return 0
  fi

  . /etc/os-release 2>/dev/null || true
  if [ "${ID:-}" = "ubuntu" ] || [ "${ID_LIKE:-}" = "ubuntu" ]; then
    log "Ubuntu обнаружена — ставлю AmneziaWG из PPA ppa:amnezia/ppa"
    apt-get update -y
    apt-get install -y software-properties-common
    add-apt-repository -y ppa:amnezia/ppa
    apt-get update -y
    apt-get install -y amneziawg amneziawg-tools
  else
    cat >&2 <<EOF

[!] Автоустановка сделана только для Ubuntu (PPA).
    Для ${ID:-этой ОС} поставьте бинарники AmneziaWG одним из способов:

    a) Готовый установщик (Ubuntu/Debian):
         git clone https://github.com/bivlked/amneziawg-installer
         sudo bash amneziawg-installer/install_amneziawg_en.sh --yes
       затем повторно запустите ЭТОТ скрипт с --skip-install, чтобы применить
       конфиг под Xray (без NAT) и наши параметры.

    b) Вручную из исходников:
         apt-get install -y build-essential dkms git linux-headers-\$(uname -r)
         git clone https://github.com/amnezia-vpn/amneziawg-tools
         make -C amneziawg-tools/src && make -C amneziawg-tools/src install
         # + модуль ядра: https://github.com/amnezia-vpn/amneziawg-linux-kernel-module

    После установки: sudo bash $0 --skip-install
EOF
    exit 1
  fi
}

[ "$SKIP_INSTALL" -eq 1 ] || install_binaries
command -v awg >/dev/null 2>&1 || { echo "awg не найден после установки." >&2; exit 1; }

# ---------------------------------------------------------------------------
# 2) Автоопределение внешнего интерфейса и публичного адреса
# ---------------------------------------------------------------------------
WAN_IF="$(ip route show default 2>/dev/null | awk '/default/{print $5; exit}')"
WAN_IF="${WAN_IF:-eth0}"
if [ -z "$AWG_ENDPOINT" ]; then
  AWG_ENDPOINT="$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1); exit}}')"
  # Если это приватный адрес (сервер за NAT), попробуем внешний IP.
  case "$AWG_ENDPOINT" in
    10.*|172.1[6-9].*|172.2[0-9].*|172.3[0-1].*|192.168.*|"")
      AWG_ENDPOINT="$(curl -fsS --max-time 5 https://api.ipify.org 2>/dev/null || echo "$AWG_ENDPOINT")" ;;
  esac
fi
log "Внешний интерфейс: ${WAN_IF} | Endpoint для клиентов: ${AWG_ENDPOINT}:${AWG_PORT}"

# ---------------------------------------------------------------------------
# 3) Генерация ключей и параметров обфускации AmneziaWG 2.0
# ---------------------------------------------------------------------------
rand() { # rand MIN MAX (включительно)
  local min=$1 max=$2 r
  r=$(( ( (RANDOM<<15) | RANDOM ) % (max-min+1) + min ))
  echo "$r"
}

if [ -f "$AWG_CONF" ]; then
  log "Найден существующий ${AWG_CONF} — не перезаписываю. Переиспользую его параметры."
else
  log "Генерирую ключи и параметры обфускации AmneziaWG 2.0"
  umask 077
  mkdir -p "$AWG_CONF_DIR"
  SRV_PRIV="$(awg genkey)"
  SRV_PUB="$(echo "$SRV_PRIV" | awg pubkey)"

  Jc=$(rand 3 8)
  Jmin=$(rand 40 80)
  Jmax=$(( Jmin + $(rand 40 120) ))
  S1=$(rand 15 150)
  S2=$(rand 15 150); while [ "$S2" -eq "$((S1+56))" ]; do S2=$(rand 15 150); done  # S1+56 != S2
  S3=$(rand 0 64)
  S4=$(rand 0 32)
  # H1..H4 — четыре различных значения в безопасном диапазоне (< INT32_MAX).
  H1=$(rand 100000 500000000)
  H2=$(rand 500000001 1000000000)
  H3=$(rand 1000000001 1500000000)
  H4=$(rand 1500000001 2100000000)
  I1="<r 128>"   # CPS AmneziaWG 2.0 (без I1 клиент падает в режим AWG 1.0)

  cat > "$AWG_CONF" <<EOF
# AmneziaWG сервер awg0 — трафик клиентов уходит в Xray (tun xray0), БЕЗ NAT.
# Параметры обфускации (Jc..I1) должны совпадать на сервере и всех клиентах.
[Interface]
PrivateKey = ${SRV_PRIV}
Address = ${AWG_SERVER_IP}/24
ListenPort = ${AWG_PORT}
MTU = 1280

Jc = ${Jc}
Jmin = ${Jmin}
Jmax = ${Jmax}
S1 = ${S1}
S2 = ${S2}
S3 = ${S3}
S4 = ${S4}
H1 = ${H1}
H2 = ${H2}
H3 = ${H3}
H4 = ${H4}
I1 = ${I1}

# ВАЖНО: НЕТ строк PostUp с MASQUERADE. Выпуск в интернет делает Xray
# (его outbound), а не этот интерфейс. Форвардинг awg0<->xray0 включает
# setup-bridge.sh / awg-xray-bridge.service.

# --- Клиенты добавляются ниже блоками [Peer] через add-client.sh ---
EOF
  chmod 600 "$AWG_CONF"
  log "Записан ${AWG_CONF}"
fi

# ---------------------------------------------------------------------------
# 4) Сохраняем общие параметры для bridge.sh и add-client.sh
# ---------------------------------------------------------------------------
SRV_PUB_SAVED="$(awk -F' = ' '/^PrivateKey/{print $2}' "$AWG_CONF" | awg pubkey)"
mkdir -p "$ENV_DIR"
cat > "$ENV_FILE" <<EOF
# Общие параметры моста AmneziaWG -> Xray. Источник истины — awg0.conf.
AWG_IF="${AWG_IF}"
AWG_SUBNET="${AWG_SUBNET}"
AWG_SERVER_IP="${AWG_SERVER_IP}"
AWG_PORT="${AWG_PORT}"
AWG_ENDPOINT="${AWG_ENDPOINT}"
AWG_SERVER_PUBKEY="${SRV_PUB_SAVED}"
WAN_IF="${WAN_IF}"
TUN_IF="xray0"
TUN_ADDR="172.16.250.1/30"
RT_TABLE="100"
RULE_PRIO="100"
EOF
chmod 600 "$ENV_FILE"
log "Параметры сохранены в ${ENV_FILE}"

# ---------------------------------------------------------------------------
# 5) Включаем сервис awg-quick@awg0
# ---------------------------------------------------------------------------
log "Включаю и запускаю awg-quick@${AWG_IF}"
systemctl enable "awg-quick@${AWG_IF}" >/dev/null 2>&1 || true
# Перезапуск подхватит правки конфига, если сервис уже работал.
systemctl restart "awg-quick@${AWG_IF}"
sleep 1
if ip link show "$AWG_IF" >/dev/null 2>&1; then
  log "Интерфейс ${AWG_IF} поднят:"
  awg show "$AWG_IF" | sed 's/^/    /'
else
  echo "[!] Интерфейс ${AWG_IF} не поднялся — проверьте: journalctl -u awg-quick@${AWG_IF}" >&2
fi

cat <<EOF

Готово. Дальше:
  1) Создайте в 3x-ui входящий 'tun' с именем xray0 (см. xray0-tun-inbound.json).
  2) Настройте мост:            sudo bash setup-bridge.sh
  3) Откройте UDP-порт ${AWG_PORT} в фаерволе/облаке.
  4) Добавьте клиента:          sudo bash add-client.sh phone
EOF
