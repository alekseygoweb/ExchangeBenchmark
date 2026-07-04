# AmneziaWG → Xray (tun `xray0`) на сервере с 3x-ui

Как добавить **AmneziaWG 2.0** на сервер, где уже работает панель
[3x-ui](https://github.com/MHSanaei/3x-ui) (Xray-core), и **пустить трафик
клиентов AmneziaWG через действующий Xray** — заворачивая его во входящий
`tun`-инбаунд `xray0`.

3x-ui сам не умеет AmneziaWG, поэтому AmneziaWG ставится рядом (ядро/DKMS +
`amneziawg-tools`), а стык с Xray делается на уровне маршрутизации Linux.

---

## Идея

```
                            сервер (3x-ui / Xray)
 клиент AmneziaWG                                              интернет
  (телефон/ПК)                                              (или upstream)
      │  обфусц. UDP        ┌───────────────────────────┐        ▲
      │  :39743             │  awg0  10.9.9.1/24         │        │
      └────────────────────►│   │ (расшифровка)         │        │
                            │   ▼                        │        │
                            │  ip rule: from 10.9.9.0/24 │        │
                            │      → table 100           │        │
                            │  table 100: default        │        │
                            │      dev xray0             │        │
                            │   │                        │        │
                            │   ▼                        │        │
                            │  xray0 ── Xray tun-inbound ─┼─ routing → outbound ─┘
                            │        (читает пакеты,      │        (freedom/цепочка)
                            │         сам делает egress)  │        eth0
                            └───────────────────────────┘
```

- **AmneziaWG** отвечает за обфусцированный, DPI-устойчивый участок
  «клиент → сервер».
- **Xray** отвечает за маршрутизацию и выход в интернет: трафик клиентов
  попадает в его `tun`-инбаунд `xray0` и обрабатывается его правилами и
  исходящими (`freedom` напрямую или цепочка на upstream-сервер).

### Почему это работает (путь пакета)

Клиент открывает `example.com:443`:

1. AmneziaWG на сервере расшифровывает пакет → он появляется на `awg0`
   (`src=10.9.9.2, dst=example.com`).
2. `ip rule from 10.9.9.0/24 lookup 100` → таблица `100`, где
   `default dev xray0`. Пакет форвардится в `xray0`.
3. Xray читает пакет из `xray0`, поднимает соединение и по своим правилам
   отправляет его через нужный **outbound** (по умолчанию — прямой `freedom`,
   т.е. выход с IP сервера; либо цепочка на ваш upstream).
4. Ответ Xray пишет обратно в `xray0` (`dst=10.9.9.2`). `main`-таблица знает
   `10.9.9.0/24 dev awg0`, пакет уходит в `awg0` и шифруется обратно клиенту.

**Петли нет:** собственные сокеты Xray (его outbound-соединения) имеют
`src = IP сервера`, а не `10.9.9.x`, поэтому под `ip rule` не попадают и
уходят через обычный маршрут (`eth0`). Именно поэтому важно, что в `xray0`
заворачивается **только** подсеть клиентов, а не `0.0.0.0/0`.

> Xray-core `tun` намеренно **не** прописывает маршруты и iptables сам
> («всё это — ваша ответственность»), поэтому мост настраивается скриптами ниже.

---

## Что в этой папке

| Файл | Назначение |
|------|-----------|
| `install-amneziawg.sh` | Ставит AmneziaWG 2.0, генерирует ключи и параметры обфускации, пишет серверный `awg0.conf` **без NAT**, включает `awg-quick@awg0`. |
| `xray0-tun-inbound.json` | Конфиг входящего Xray `tun` (`xray0`) для 3x-ui. |
| `setup-bridge.sh` | Ставит sysctl + systemd-юниты моста `awg0 → xray0`. |
| `add-client.sh` | Добавляет клиента (ключи, `[Peer]`, клиентский `.conf` + QR). |
| `bridge/99-awg-xray.conf` | `ip_forward`, `rp_filter=loose`. |
| `bridge/awg-xray-bridge.sh` | Рантайм: `ip rule`, `ip route table 100`, `FORWARD`. |
| `bridge/awg-xray-bridge.{service,path}` | Применяют мост при появлении `xray0` и при каждом рестарте Xray. |

Все скрипты идемпотентны и запускаются от `root`.

---

## Предварительно

- Сервер с работающей панелью **3x-ui** (Xray-core с поддержкой `tun` —
  в списке протоколов инбаунда есть `tun`, как на скриншоте).
- **Ubuntu** (для автоустановки из PPA). Debian/прочее — см. примечание в
  `install-amneziawg.sh` (используйте установщик
  [bivlked/amneziawg-installer](https://github.com/bivlked/amneziawg-installer)
  или сборку из исходников, затем `--skip-install`).
- Свободный **UDP-порт** для AmneziaWG (по умолчанию `39743`) — открыть в
  фаерволе VPS/облака.
- Доступ по SSH с `sudo`.

Скопируйте эту папку на сервер (например, `scp -r amneziawg-xray root@SERVER:`).

---

## Установка по шагам

### 1. Поставить AmneziaWG и создать серверный конфиг

```bash
cd amneziawg-xray
sudo bash install-amneziawg.sh --port 39743
# сервер за NAT? укажите внешний адрес явно:
# sudo bash install-amneziawg.sh --port 39743 --endpoint 203.0.113.10
```

Скрипт: поставит `amneziawg` + `amneziawg-tools`, сгенерирует ключи и
параметры обфускации AmneziaWG 2.0 (Jc/Jmin/Jmax, S1–S4, H1–H4, `I1=<r 128>`),
запишет `/etc/amnezia/amneziawg/awg0.conf` **без строк MASQUERADE**, включит
`awg-quick@awg0` и сохранит общие параметры в `/etc/awg-xray/bridge.env`.

### 2. Создать входящий `tun` `xray0` в 3x-ui

В панели: **Inbounds → Создать подключение → Протокол = `tun`**, имя
интерфейса **`xray0`**, MTU **1500**.

Если UI не даёт задать все поля — откройте **«Расширенный шаблон»** инбаунда
(или **Настройки Xray → шаблон конфигурации**) и вставьте блок из
[`xray0-tun-inbound.json`](./xray0-tun-inbound.json). Сохраните и
**перезапустите Xray** из панели — появится интерфейс `xray0`.

Проверка на сервере:

```bash
ip link show xray0        # интерфейс существует
```

### 3. Настроить мост `awg0 → xray0`

```bash
sudo bash setup-bridge.sh
```

Ставит `ip_forward`/`rp_filter`, скрипт моста и systemd-юниты. Юнит
`awg-xray-bridge.path` следит за `xray0` и **автоматически** восстанавливает
маршрут при каждом рестарте Xray (панель пересоздаёт `xray0`).

### 4. Открыть порт AmneziaWG

```bash
sudo ufw allow 39743/udp        # или правило в фаерволе облака/провайдера
```

### 5. Добавить клиента

```bash
sudo bash add-client.sh phone
# по желанию: --dns 1.1.1.1  --ip 10.9.9.50
```

Появится клиентский конфиг в `/root/awg-clients/phone.conf` и QR в терминале.
Импортируйте его в клиент **AmneziaWG/AmneziaVPN** (не в обычный WireGuard —
нужны параметры обфускации).

---

## Куда именно выходит трафик клиентов?

Трафик клиентов подчиняется **маршрутизации Xray в 3x-ui**:

- **По умолчанию** — прямой выход с IP сервера (исходящий `freedom`/`direct`).
  Клиенты AmneziaWG получают тот же egress, что и остальные пользователи Xray.
- **Цепочка на upstream** — если в 3x-ui задан исходящий на другой сервер
  (VLESS/VMess/Trojan/WireGuard/WARP…), направьте туда трафик `tun`-инбаунда
  правилом маршрутизации (по `inboundTag: ["tun-xray0"]` или по домену/гео).
  Тогда AmneziaWG обфусцирует участок «клиент→сервер», а Xray уводит трафик
  дальше — удобно для обхода блокировок.

Так как в инбаунде включён `sniffing`, правила Xray видят домен (SNI), и можно
роутить клиентов по доменам/гео через разные исходящие.

---

## Проверка

На сервере, после подключения клиента:

```bash
awg show awg0                     # у пира растут rx/tx, свежий handshake
ip rule | grep 10.9.9             # from 10.9.9.0/24 lookup 100
ip route show table 100           # default dev xray0
systemctl status awg-xray-bridge.path
```

На клиенте: открывается интернет, а «мой IP» показывает egress Xray
(IP сервера или upstream). DNS-запросы также идут через туннель.

---

## Диагностика

| Симптом | Причина / решение |
|--------|-------------------|
| Handshake есть (`awg show`), но интернета нет | Не создан/не запущен `tun` `xray0`, либо не отработал мост. Проверьте `ip link show xray0`, `ip route show table 100`, `systemctl start awg-xray-bridge.service`. |
| Пакеты уходят, ответов нет | Строгий `rp_filter` рубит обратный трафик из `xray0`. Убедитесь, что `sysctl net.ipv4.conf.all.rp_filter = 2` (файл `bridge/99-awg-xray.conf`). |
| Трафик уходит, но не через Xray (обычный egress) | Проверьте, что нет лишнего `MASQUERADE` от `awg0` и что `ip rule` действительно ведёт в `table 100 → dev xray0`. |
| Зацикливание / Xray не достаёт uplink | В `xray0` не должно попадать `0.0.0.0/0` (только `from 10.9.9.0/24`). Как страховка — задайте `autoOutboundsInterface: "eth0"` в инбаунде. |
| После рестарта Xray интернет у клиентов пропал | `xray0` пересоздался. Юнит `awg-xray-bridge.path` должен вернуть маршрут; если нет — `systemctl status awg-xray-bridge.path` и `journalctl -u awg-xray-bridge.service`. |
| Медленно/рвётся на части сайтов | MTU. Оставьте `MTU=1280` в конфигах AmneziaWG (уже задано). |
| Клиент не коннектится вовсе | Параметры обфускации на клиенте и сервере должны совпадать байт-в-байт; порт UDP открыт; клиент — именно AmneziaWG, а не WireGuard. |
| `FORWARD` рубит трафик (политика DROP) | Мост добавляет `ACCEPT` для `awg0↔xray0`. Проверьте `iptables -S FORWARD`. |

Логи: `journalctl -u awg-quick@awg0`, `journalctl -u awg-xray-bridge.service`,
логи Xray в 3x-ui.

---

## Персистентность и откат

- `awg-quick@awg0` и `awg-xray-bridge.path` включены (`enable`) → переживают
  перезагрузку. `rp_filter`/`ip_forward` — в `/etc/sysctl.d/`.
- **Откат моста:**
  ```bash
  systemctl disable --now awg-xray-bridge.path awg-xray-bridge.service
  ip rule del from 10.9.9.0/24 table 100 2>/dev/null || true
  ip route flush table 100 2>/dev/null || true
  rm -f /etc/sysctl.d/99-awg-xray.conf /usr/local/sbin/awg-xray-bridge.sh \
        /etc/systemd/system/awg-xray-bridge.{service,path}
  systemctl daemon-reload
  ```
- **Удалить AmneziaWG:** `systemctl disable --now awg-quick@awg0`, затем удалить
  пакеты `amneziawg amneziawg-tools`. Инбаунд `xray0` удаляется из 3x-ui.

---

## Замечания

- **IPv6.** Схема показана для IPv4. Для IPv6 добавьте клиентам адрес из ULA
  (напр. `fd00:9:9::/64`), аналогичные `ip -6 rule` / `ip -6 route table 100`
  и `net.ipv6.conf.all.forwarding=1` (уже включён).
- **Безопасность.** `awg0.conf` и клиентские `.conf` содержат приватные ключи —
  права `600`, не коммитьте их. В репозитории только шаблоны/скрипты, без секретов.
- **Альтернатива без `tun`.** Если версия Xray в вашей 3x-ui не имеет `tun`,
  тот же эффект даёт `dokodemo-door` + TPROXY (`iptables -t mangle`/`ip rule`
  fwmark) — более традиционный «прозрачный прокси», но настройка объёмнее.
  Раз в панели есть `tun` — используем его как более простой путь.

---

## Источники

- 3x-ui (список протоколов включает `Tun`): <https://github.com/MHSanaei/3x-ui>
- Xray-core `tun` inbound: <https://github.com/XTLS/Xray-core/tree/main/proxy/tun>,
  <https://xtls.github.io/en/config/inbounds/tun.html>
- AmneziaWG (параметры/установка): <https://docs.amnezia.org/documentation/amnezia-wg/>,
  <https://github.com/amnezia-vpn/amneziawg-tools>
- Установщик AmneziaWG 2.0: <https://github.com/bivlked/amneziawg-installer>
