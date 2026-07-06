#!/usr/bin/env bash
#
# awg-params.sh — общая генерация «правильных» параметров обфускации AmneziaWG 2.0.
# Подключается через `source` из install-amneziawg.sh и update-obfuscation.sh.
#
# Даёт функции:
#   awg_rand MIN MAX         — случайное число в диапазоне (до < 2^31)
#   awg_gen_h_ranges         — ставит глобалы H1..H4 как диапазоны "A-B"
#   awg_gen_cps_i1           — ставит глобал I1 = крафтовый CPS-пакет (DNS-ответ)
#   awg_gen_jc_s             — ставит Jc/Jmin/Jmax/S1..S4 (валидные значения)
#
# Требования спецификации AWG 2.0, которые здесь соблюдены:
#   * H1..H4 не пересекаются и <= INT32_MAX (2147483647);
#   * S1 + 56 != S2;
#   * I1 включает CPS (без него клиент откатывается в режим AWG 1.0).

# 45-битный источник случайности (RANDOM — только 15 бит), хватает для < INT32_MAX.
awg_rand() { # awg_rand MIN MAX  (включительно)
  local min=$1 max=$2
  echo $(( ( (RANDOM<<30) | (RANDOM<<15) | RANDOM ) % (max - min + 1) + min ))
}

# H1..H4 как непересекающиеся возрастающие диапазоны в четырёх «полосах»
# [16 .. INT32_MAX], каждый вида "start-end".
awg_gen_h_ranges() {
  local lo hi width start i
  local bands_lo=(16         536870912  1073741824 1610612736)
  local bands_hi=(536870911  1073741823 1610612735 2147483647)
  for i in 0 1 2 3; do
    lo=${bands_lo[$i]}; hi=${bands_hi[$i]}
    width=$(awg_rand 5000000 80000000)              # ширина диапазона
    start=$(awg_rand "$lo" $(( hi - width - 1 )) )
    eval "H$((i+1))=\"${start}-$(( start + width ))\""
  done
}

# I1 = крафтовый CPS-пакет, который парсится как DNS-ответ A-записи для
# www.google.com -> 142.250.x.y (последние два октета случайны на сервер).
# <r 2> = случайный 2-байтный DNS transaction id (awg подставляет в рантайме),
# далее <b 0x...> = тело DNS-ответа. Такой первый пакет выглядит как обычный DNS.
awg_gen_cps_i1() {
  local a b
  a=$(printf '%02x' "$(awg_rand 0 255)")
  b=$(printf '%02x' "$(awg_rand 0 255)")
  #  hdr:8180 qd:0001 an:0001 ns:0000 ar:0000
  #  q:  03"www" 06"google" 03"com" 00  type:0001(A) class:0001(IN)
  #  a:  c00c type:0001 class:0001 ttl:0000012c rdlen:0004 ip:8efa<a><b>
  I1="<r 2><b 0x818000010001000000000377777706676f6f676c6503636f6d0000010001c00c000100010000012c00048efa${a}${b}>"
}

# Джанк-пакеты (Jc/Jmin/Jmax) и паддинг сообщений (S1..S4) — валидные значения.
awg_gen_jc_s() {
  Jc=$(awg_rand 3 8)
  Jmin=$(awg_rand 40 80)
  Jmax=$(( Jmin + $(awg_rand 40 120) ))
  S1=$(awg_rand 15 150)
  S2=$(awg_rand 15 150); while [ "$S2" -eq "$((S1 + 56))" ]; do S2=$(awg_rand 15 150); done
  S3=$(awg_rand 0 64)
  S4=$(awg_rand 0 32)
}
