#!/usr/bin/env bash
#
# setup-bridge.sh
# ---------------
# Устанавливает и включает мост AmneziaWG(awg0) -> Xray(xray0):
#   * /etc/sysctl.d/99-awg-xray.conf   (ip_forward, rp_filter=loose)
#   * /usr/local/sbin/awg-xray-bridge.sh
#   * systemd: awg-xray-bridge.service + awg-xray-bridge.path
#
# Юнит .path следит за появлением интерфейса xray0 и (пере)настраивает
# маршрутизацию при каждом старте/рестарте Xray. Идемпотентно.
#
set -euo pipefail
[ "$(id -u)" -eq 0 ] || { echo "Запустите от root (sudo)." >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${SCRIPT_DIR}/bridge"
log() { echo -e "\n\033[1;36m==>\033[0m $*"; }

for f in 99-awg-xray.conf awg-xray-bridge.sh awg-xray-bridge.service awg-xray-bridge.path; do
  [ -f "${SRC}/${f}" ] || { echo "Не найден ${SRC}/${f}" >&2; exit 1; }
done

log "Параметры ядра -> /etc/sysctl.d/99-awg-xray.conf"
install -m 0644 "${SRC}/99-awg-xray.conf" /etc/sysctl.d/99-awg-xray.conf
sysctl --system >/dev/null

log "Скрипт моста -> /usr/local/sbin/awg-xray-bridge.sh"
install -m 0755 "${SRC}/awg-xray-bridge.sh" /usr/local/sbin/awg-xray-bridge.sh

log "systemd-юниты -> /etc/systemd/system/"
install -m 0644 "${SRC}/awg-xray-bridge.service" /etc/systemd/system/awg-xray-bridge.service
install -m 0644 "${SRC}/awg-xray-bridge.path"    /etc/systemd/system/awg-xray-bridge.path
systemctl daemon-reload

log "Включаю awg-xray-bridge.path (следит за xray0)"
systemctl enable --now awg-xray-bridge.path

# Если xray0 уже есть — применим мост немедленно.
if ip link show xray0 >/dev/null 2>&1; then
  log "xray0 уже существует — применяю мост сейчас"
  systemctl start awg-xray-bridge.service || true
else
  echo "[i] Интерфейс xray0 пока не найден. Создайте tun-инбаунд xray0 в 3x-ui —"
  echo "    мост применится автоматически, как только Xray его поднимет."
fi

log "Готово. Проверка после появления xray0:"
cat <<'EOF'
    ip rule                       # должна быть строка: from 10.9.9.0/24 lookup 100
    ip route show table 100       # default dev xray0
    systemctl status awg-xray-bridge.path
EOF
