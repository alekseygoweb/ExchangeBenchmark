#!/usr/bin/env bash
#
# add-client.sh NAME [--dns 1.1.1.1] [--ip 10.9.9.X]
# --------------------------------------------------
# Добавляет клиента AmneziaWG: генерирует ключи, вписывает [Peer] в awg0.conf,
# применяет вживую (без разрыва других клиентов) и печатает клиентский конфиг +
# QR. Параметры обфускации (Jc..I1) берутся из серверного awg0.conf — они обязаны
# совпадать байт-в-байт, иначе сервер не разберёт handshake.
#
# AllowedIPs клиента = 0.0.0.0/0, ::/0 -> весь трафик идёт на сервер и дальше в Xray.
#
set -euo pipefail
[ "$(id -u)" -eq 0 ] || { echo "Запустите от root (sudo)." >&2; exit 1; }

NAME=""; DNS="1.1.1.1"; FORCE_IP=""
while [ $# -gt 0 ]; do
  case "$1" in
    --dns) DNS="$2"; shift 2 ;;
    --ip)  FORCE_IP="$2"; shift 2 ;;
    -*) echo "Неизвестный флаг: $1" >&2; exit 1 ;;
    *) NAME="$1"; shift ;;
  esac
done
[ -n "$NAME" ] || { echo "Использование: $0 NAME [--dns 1.1.1.1] [--ip 10.9.9.X]" >&2; exit 1; }

ENV_FILE="/etc/awg-xray/bridge.env"
[ -f "$ENV_FILE" ] && . "$ENV_FILE"
AWG_IF="${AWG_IF:-awg0}"
AWG_CONF="/etc/amnezia/amneziawg/${AWG_IF}.conf"
[ -f "$AWG_CONF" ] || { echo "Не найден ${AWG_CONF}. Сначала запустите install-amneziawg.sh" >&2; exit 1; }

OUT_DIR="/root/awg-clients"
mkdir -p "$OUT_DIR"; umask 077

# --- Параметры обфускации и данные сервера из awg0.conf / env ---
# Значение — всё после первого " = " (устойчиво к '=' в base64-ключах и пробелам в "I1 = <r 128>").
getp() { awk -v k="$1" '$1==k { i=index($0," = "); if(i){print substr($0,i+3)}; exit }' "$AWG_CONF"; }
Jc=$(getp Jc); Jmin=$(getp Jmin); Jmax=$(getp Jmax)
S1=$(getp S1); S2=$(getp S2); S3=$(getp S3); S4=$(getp S4)
H1=$(getp H1); H2=$(getp H2); H3=$(getp H3); H4=$(getp H4)
I1=$(getp I1)
SRV_PORT="${AWG_PORT:-$(getp ListenPort)}"
SRV_PUB="${AWG_SERVER_PUBKEY:-$(getp PrivateKey | awg pubkey)}"
ENDPOINT="${AWG_ENDPOINT:?Не задан AWG_ENDPOINT — перезапустите install-amneziawg.sh}"
SUBNET_BASE="$(echo "${AWG_SUBNET%/*}" | cut -d. -f1-3)"   # 10.9.9.0/24 -> 10.9.9

# --- Выбор свободного IP клиента ---
if [ -n "$FORCE_IP" ]; then
  CLIENT_IP="$FORCE_IP"
else
  # '|| true' — у первого клиента совпадений ещё нет (grep -> exit 1), не роняем set -e.
  last=$(grep -oE "AllowedIPs *= *${SUBNET_BASE}\.[0-9]+" "$AWG_CONF" \
          | grep -oE '[0-9]+$' | sort -n | tail -n1 || true)
  next=$(( ${last:-1} + 1 ))
  [ "$next" -le 254 ] || { echo "Свободные адреса в подсети закончились." >&2; exit 1; }
  CLIENT_IP="${SUBNET_BASE}.${next}"
fi

# --- Ключи клиента ---
C_PRIV="$(awg genkey)"
C_PUB="$(echo "$C_PRIV" | awg pubkey)"
C_PSK="$(awg genpsk)"

# --- Вписываем [Peer] в серверный конфиг и применяем вживую ---
cat >> "$AWG_CONF" <<EOF

# client: ${NAME}
[Peer]
PublicKey = ${C_PUB}
PresharedKey = ${C_PSK}
AllowedIPs = ${CLIENT_IP}/32
EOF

# Применяем без разрыва существующих сессий.
awg syncconf "$AWG_IF" <(awg-quick strip "$AWG_IF")

# --- Клиентский конфиг ---
CLIENT_CONF="${OUT_DIR}/${NAME}.conf"
cat > "$CLIENT_CONF" <<EOF
[Interface]
PrivateKey = ${C_PRIV}
Address = ${CLIENT_IP}/32
DNS = ${DNS}
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

[Peer]
PublicKey = ${SRV_PUB}
PresharedKey = ${C_PSK}
Endpoint = ${ENDPOINT}:${SRV_PORT}
AllowedIPs = 0.0.0.0/0, ::/0
PersistentKeepalive = 25
EOF
chmod 600 "$CLIENT_CONF"

echo "Клиент '${NAME}' добавлен: ${CLIENT_IP}  ->  ${CLIENT_CONF}"
echo
if command -v qrencode >/dev/null 2>&1; then
  qrencode -t ANSIUTF8 < "$CLIENT_CONF"
else
  echo "(поставьте qrencode для QR: apt-get install -y qrencode)"
  echo "--- ${NAME}.conf ---"; cat "$CLIENT_CONF"
fi
