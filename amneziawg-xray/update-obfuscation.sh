#!/usr/bin/env bash
#
# update-obfuscation.sh
# ---------------------
# Обновляет параметры обфускации AmneziaWG до «официального» вида AWG 2.0
# (H1–H4 как диапазоны + крафтовый CPS-пакет I1) СРАЗУ на сервере и во всех
# ранее выданных клиентских конфигах — иначе рукопожатие сломается.
#
# Параметры должны совпадать байт-в-байт на сервере и клиентах, поэтому:
#   * делаем бэкап awg0.conf и всех /root/awg-clients/*.conf;
#   * генерируем (или берём из --from) новый набор;
#   * переписываем нужные строки в сервере и КАЖДОМ клиентском .conf;
#   * перезапускаем awg-quick@awg0.
# После этого КАЖДОГО клиента нужно заново импортировать (файл/QR обновлены).
#
# Режимы:
#   (по умолчанию)   заново генерирует H1–H4 (диапазоны) и I1 (CPS-DNS),
#                    Jc/Jmin/Jmax/S1–S4 оставляет как есть.
#   --all            дополнительно перегенерирует Jc/Jmin/Jmax/S1–S4.
#   --from FILE      берёт весь набор (H,I1,Jc,S) из готового AWG-конфига FILE
#                    (например, сгенерированного официальным приложением Amnezia).
#   --yes            не спрашивать подтверждение.
#
set -euo pipefail
[ "$(id -u)" -eq 0 ] || { echo "Запустите от root (sudo)." >&2; exit 1; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "${SCRIPT_DIR}/awg-params.sh"

MODE_ALL=0; FROM=""; ASSUME_YES=0
while [ $# -gt 0 ]; do
  case "$1" in
    --all) MODE_ALL=1; shift ;;
    --from) FROM="$2"; shift 2 ;;
    --yes|-y) ASSUME_YES=1; shift ;;
    *) echo "Неизвестный аргумент: $1" >&2; exit 1 ;;
  esac
done

AWG_IF="awg0"
AWG_CONF="/etc/amnezia/amneziawg/${AWG_IF}.conf"
CLIENTS_DIR="/root/awg-clients"
[ -f "$AWG_CONF" ] || { echo "Не найден ${AWG_CONF}. Сначала install-amneziawg.sh" >&2; exit 1; }

log() { echo -e "\n\033[1;36m==>\033[0m $*"; }

# Значение параметра из файла: всё после первого " = ".
getp() { awk -v k="$1" '$1==k { i=index($0," = "); if(i){print substr($0,i+3)}; exit }' "$2"; }

# ---------------------------------------------------------------------------
# 1) Определяем новый набор параметров
# ---------------------------------------------------------------------------
# Стартуем со ВСЕХ текущих значений сервера, затем переопределяем нужные.
Jc=$(getp Jc "$AWG_CONF");   Jmin=$(getp Jmin "$AWG_CONF"); Jmax=$(getp Jmax "$AWG_CONF")
S1=$(getp S1 "$AWG_CONF"); S2=$(getp S2 "$AWG_CONF"); S3=$(getp S3 "$AWG_CONF"); S4=$(getp S4 "$AWG_CONF")

if [ -n "$FROM" ]; then
  [ -f "$FROM" ] || { echo "Файл --from не найден: $FROM" >&2; exit 1; }
  log "Беру набор параметров из ${FROM}"
  Jc=$(getp Jc "$FROM"); Jmin=$(getp Jmin "$FROM"); Jmax=$(getp Jmax "$FROM")
  S1=$(getp S1 "$FROM"); S2=$(getp S2 "$FROM"); S3=$(getp S3 "$FROM"); S4=$(getp S4 "$FROM")
  H1=$(getp H1 "$FROM"); H2=$(getp H2 "$FROM"); H3=$(getp H3 "$FROM"); H4=$(getp H4 "$FROM")
  I1=$(getp I1 "$FROM")
  [ -n "$H1$H2$H3$H4$I1" ] || { echo "В ${FROM} не нашлись H1..H4/I1." >&2; exit 1; }
else
  log "Генерирую новый набор: H1–H4 диапазоны + I1 CPS-DNS"
  awg_gen_h_ranges
  awg_gen_cps_i1
  [ "$MODE_ALL" -eq 1 ] && { log "Также перегенерирую Jc/Jmin/Jmax/S1–S4"; awg_gen_jc_s; }
fi

echo
echo "Новые параметры обфускации:"
printf '  Jc=%s Jmin=%s Jmax=%s\n  S1=%s S2=%s S3=%s S4=%s\n' "$Jc" "$Jmin" "$Jmax" "$S1" "$S2" "$S3" "$S4"
printf '  H1=%s\n  H2=%s\n  H3=%s\n  H4=%s\n  I1=%s\n' "$H1" "$H2" "$H3" "$H4" "$I1"
echo
mapfile -t CLIENT_FILES < <(ls -1 "${CLIENTS_DIR}"/*.conf 2>/dev/null || true)
echo "Будут обновлены: сервер ${AWG_CONF} и ${#CLIENT_FILES[@]} клиентских конфигов."
echo "ВНИМАНИЕ: соединения разорвутся до переимпорта конфигов на клиентах."
if [ "$ASSUME_YES" -ne 1 ]; then
  read -r -p "Продолжить? [y/N] " ans
  case "$ans" in y|Y|yes|да) ;; *) echo "Отменено."; exit 0 ;; esac
fi

# ---------------------------------------------------------------------------
# 2) Бэкап
# ---------------------------------------------------------------------------
TS="$(date +%Y%m%d-%H%M%S)"
BAK="/root/awg-obf-backup-${TS}"
mkdir -p "$BAK"
cp -a "$AWG_CONF" "${BAK}/$(basename "$AWG_CONF")"
[ "${#CLIENT_FILES[@]}" -gt 0 ] && cp -a "${CLIENTS_DIR}" "${BAK}/awg-clients"
log "Бэкап -> ${BAK}"

# ---------------------------------------------------------------------------
# 3) Переписываем строки параметров в файле (awk, только в [Interface])
# ---------------------------------------------------------------------------
apply_to() { # apply_to FILE
  local f="$1" tmp="$1.tmp.$$"
  awk -v Jc="$Jc" -v Jmin="$Jmin" -v Jmax="$Jmax" \
      -v S1="$S1" -v S2="$S2" -v S3="$S3" -v S4="$S4" \
      -v H1="$H1" -v H2="$H2" -v H3="$H3" -v H4="$H4" -v I1="$I1" '
    function rep(key, val,   pfx) { pfx = key " ="; return (substr($0,1,length(pfx))==pfx) }
    rep("Jc")   {print "Jc = "   Jc;   next}
    rep("Jmin") {print "Jmin = " Jmin; next}
    rep("Jmax") {print "Jmax = " Jmax; next}
    rep("S1")   {print "S1 = "   S1;   next}
    rep("S2")   {print "S2 = "   S2;   next}
    rep("S3")   {print "S3 = "   S3;   next}
    rep("S4")   {print "S4 = "   S4;   next}
    rep("H1")   {print "H1 = "   H1;   next}
    rep("H2")   {print "H2 = "   H2;   next}
    rep("H3")   {print "H3 = "   H3;   next}
    rep("H4")   {print "H4 = "   H4;   next}
    rep("I1")   {print "I1 = "   I1;   next}
    {print}
  ' "$f" > "$tmp"
  # чтобы не затереть файл при сбое awk
  if [ -s "$tmp" ]; then cat "$tmp" > "$f"; rm -f "$tmp"; else rm -f "$tmp"; echo "Ошибка awk на $f" >&2; exit 1; fi
}

apply_to "$AWG_CONF"
for cf in "${CLIENT_FILES[@]}"; do apply_to "$cf"; echo "  обновлён клиент: $cf"; done

# ---------------------------------------------------------------------------
# 4) Применяем на сервере
# ---------------------------------------------------------------------------
log "Перезапускаю awg-quick@${AWG_IF}"
systemctl restart "awg-quick@${AWG_IF}"
sleep 1
awg show "$AWG_IF" | sed 's/^/    /'

cat <<EOF

Готово. Параметры обновлены на сервере и в ${#CLIENT_FILES[@]} клиентских конфигах.
Бэкап: ${BAK}

Дальше:
  * Заново импортируйте КАЖДЫЙ клиентский конфиг (файлы обновлены в ${CLIENTS_DIR}).
    QR заново:  qrencode -t ANSIUTF8 -r ${CLIENTS_DIR}/ИМЯ.conf
  * Проверка:  sudo awg show awg0   (после переподключения клиента растут received И sent)

Откат:  cp ${BAK}/$(basename "$AWG_CONF") ${AWG_CONF} && \\
        cp -a ${BAK}/awg-clients/* ${CLIENTS_DIR}/ 2>/dev/null; \\
        systemctl restart awg-quick@${AWG_IF}
EOF
