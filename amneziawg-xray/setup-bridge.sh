#!/usr/bin/env bash
#
# setup-bridge.sh
# ---------------
# Устанавливает и включает мост AmneziaWG(awg0) -> Xray(xray0):
#   * /etc/sysctl.d/99-awg-xray.conf        (ip_forward, rp_filter=loose)
#   * /usr/local/sbin/awg-xray-bridge.sh
#   * systemd: awg-xray-bridge.service + awg-xray-bridge.timer
#   * udev:    /etc/udev/rules.d/99-awg-xray.rules
#
# udev-правило мгновенно (пере)настраивает маршрутизацию при каждом создании
# интерфейса xray0 (старт/рестарт Xray). Таймер раз в 30 с — страховка.
# Идемпотентно; можно запускать повторно (в т.ч. для обновления с версии .path).
#
set -euo pipefail
[ "$(id -u)" -eq 0 ] || { echo "Запустите от root (sudo)." >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC="${SCRIPT_DIR}/bridge"
log() { echo -e "\n\033[1;36m==>\033[0m $*"; }

for f in 99-awg-xray.conf awg-xray-bridge.sh awg-xray-bridge.service \
         awg-xray-bridge.timer 99-awg-xray.rules; do
  [ -f "${SRC}/${f}" ] || { echo "Не найден ${SRC}/${f}" >&2; exit 1; }
done

# --- Миграция со старого механизма (.path на sysfs — ненадёжен) ---
# Безусловно (не через if с pipefail-проверкой, которая могла пропускать блок).
log "Убираю старый awg-xray-bridge.path, если остался (заменён на udev + timer)"
systemctl disable --now awg-xray-bridge.path 2>/dev/null || true
rm -f /etc/systemd/system/awg-xray-bridge.path \
      /etc/systemd/system/*.wants/awg-xray-bridge.path

log "Параметры ядра -> /etc/sysctl.d/99-awg-xray.conf"
install -m 0644 "${SRC}/99-awg-xray.conf" /etc/sysctl.d/99-awg-xray.conf
sysctl --system >/dev/null

log "Скрипт моста -> /usr/local/sbin/awg-xray-bridge.sh"
install -m 0755 "${SRC}/awg-xray-bridge.sh" /usr/local/sbin/awg-xray-bridge.sh

log "systemd-юниты -> /etc/systemd/system/"
install -m 0644 "${SRC}/awg-xray-bridge.service" /etc/systemd/system/awg-xray-bridge.service
install -m 0644 "${SRC}/awg-xray-bridge.timer"   /etc/systemd/system/awg-xray-bridge.timer
systemctl daemon-reload

log "udev-правило -> /etc/udev/rules.d/99-awg-xray.rules"
install -m 0644 "${SRC}/99-awg-xray.rules" /etc/udev/rules.d/99-awg-xray.rules
udevadm control --reload-rules

log "Включаю таймер-страховку awg-xray-bridge.timer"
systemctl enable --now awg-xray-bridge.timer

# Применим мост немедленно (не ждём таймер), если xray0 уже есть.
if ip link show xray0 >/dev/null 2>&1; then
  log "xray0 уже существует — применяю мост сейчас"
  systemctl start awg-xray-bridge.service || true
else
  echo "[i] Интерфейс xray0 пока не найден. Создайте tun-инбаунд xray0 в 3x-ui —"
  echo "    мост применится автоматически (udev), как только Xray его поднимет."
fi

log "Готово. Проверка после появления xray0:"
cat <<'EOF'
    ip rule                        # должна быть строка: from 10.9.9.0/24 lookup 100
    ip route show table 100        # default dev xray0
    systemctl status awg-xray-bridge.timer
    systemctl list-timers awg-xray-bridge.timer
EOF
