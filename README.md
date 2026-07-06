# ExchangeBenchmark

Замер задержки торговых операций на **Binance**, **OKX**, **MEXC**,
**Binance.US**, **Bybit**, **Bitget**, **BingX** и **Coinbase** (фьючерсы и
спот, где есть), по двум транспортам — **REST API** и **WebSocket**, в двух
режимах — **обычный лимитный** ордер и **условный (trigger/algo)** ордер.

- `latency_test.py` — цикл «разместить ордер → отменить ордер», считает задержку
  размещения, отмены и суммы (мс) по каждой бирже / рынку / транспорту.
  Требует **боевых ключей** (размещает реальные ордера).
- `public_latency.py` — **публичный** замер (без ключей и без ордеров): REST RTT,
  WS-upgrade, подписка на канал, WS ping/pong RTT + резолв IP/rDNS с подсказкой
  региона/CDN. Именно им выбирают, **куда ставить VPS** (см. ниже).
- `socket_test.py` — отдельный замер времени открытия WebSocket-сокета
  (DNS + TCP + TLS + upgrade) и логина OKX.

## Где находятся сервера бирж (куда ставить VPS)

Матчинг-движки бирж разнесены по двум кластерам — США и Азия, поэтому одной
локацией не покрыть всё. Данные: официальная документация + независимые замеры +
резолв прямых (не-CDN) эндпоинтов.

| Биржа | Провайдер / регион | Город | Достоверность |
|---|---|---|---|
| Coinbase Exchange (спот) | AWS **us-east-1** | Сев. Вирджиния | ✅ офиц. + DNS |
| Binance.US | AWS **us-east-1** | Сев. Вирджиния | ✅ прямой DNS |
| Bybit | AWS **ap-southeast-1** (AZ `apse1-az2/az3`) | Сингапур | ✅ офиц. FAQ |
| MEXC | AWS **ap-northeast-1** | Токио | ⚠ замеры/доки |
| Bitget | AWS **ap-northeast-1** | Токио | ⚠ замеры |
| BingX | не публикуется (за CloudFront) | Азия? | ❌ только замер |
| Binance global | AWS **ap-northeast-1** | Токио | ✅ офиц. |
| OKX | AWS/собств. | Гонконг/Сингапур | ⚠ замеры |

Практика: **два сервера — AWS us-east-1 (Coinbase, Binance.US) и AWS Tokyo
ap-northeast-1 (Binance, MEXC, Bitget, BingX?)**, плюс, если критичен Bybit, —
Сингапур `ap-southeast-1`. Пинг по публичному REST бесполезен (все API за CDN —
меряет edge, а не движок); мерить нужно WS-подписку/ping и полный цикл ордера.
Неопределённость по MEXC/Bitget/**BingX** закрывается прогоном `public_latency.py`
с VPS в Токио/Сингапуре/us-east-1.

> ⚠️ **Скрипт работает на БОЕВЫХ счетах (PROD, РЕАЛЬНЫЕ ДЕНЬГИ).**
> Размещаются настоящие ордера и сразу отменяются. Они стоят далеко от рынка
> (non-marketable), чтобы не исполниться, объём минимальный. Весь риск на вас.

## Публичный замер задержки (без ключей) — `public_latency.py`

Отвечает на вопрос «куда ставить сервер» без ключей и без ордеров (нулевой риск).
Гоняйте с каждого кандидата-VPS (Токио / Сингапур / us-east-1) и сравнивайте.

```bash
pip install -r requirements.txt
python3 public_latency.py                       # все биржи
python3 public_latency.py --exchange bybit,mexc,bingx,bitget,coinbase,binanceus
python3 public_latency.py --resolve-only         # только IP / rDNS / регион
python3 public_latency.py --breakdown            # + DNS / TCP / TLS раздельно
python3 public_latency.py --list                 # ключи бирж
```

Что меряется по публичным эндпоинтам (аутентификация не нужна):

| Метрика | Что это |
|---|---|
| **REST RTT** | полный цикл GET к публичному эндпоинту (server time / ping) |
| **WS-upgrade** | установка WebSocket-соединения (DNS+TCP+TLS+HTTP 101) |
| **Подписка** | от `subscribe` до подтверждения/первого сообщения канала |
| **Ping/pong RTT** | app-level `ping→pong` на прогретом сокете (где биржа поддерживает; иначе управляющий WS-ping RFC6455) |

Плюс резолв IP + reverse-DNS с подсказкой региона/CDN. **Важно:** REST-эндпоинты
бирж стоят за CDN (CloudFront/Cloudflare/Akamai), поэтому REST RTT и TCP/TLS часто
меряют ближайший edge, а не движок. Для выбора региона ориентируйтесь на
**Подписку** и **Ping/pong** — они идут до origin-шлюза биржи. Прямой (не-CDN)
эндпоинт есть, например, у Coinbase (`ws-direct.exchange.coinbase.com` →
реальный us-east-1) и Binance.US (`stream.binance.us` → us-east-1).

## Быстрый старт — одна команда на всё

Лимитный бенчмарк по **всем биржам**, спот + фьючерсы, REST + WS (ключи уже в
`.env`):

```bash
LATENCY_CONFIRM=1 python3 latency_test.py --exchange both --market both --repeats 3
```

С нуля на новом сервере (Ubuntu/Debian) — одной цепочкой (ключи впишете в `.env`
на шаге `nano`):

```bash
sudo apt update && sudo apt install -y python3 python3-venv git && \
git clone https://github.com/alekseygoweb/exchangebenchmark.git && \
cd exchangebenchmark && python3 -m venv .venv && . .venv/bin/activate && \
pip install -r requirements.txt && cp .env.example .env && nano .env && \
LATENCY_CONFIRM=1 python3 latency_test.py --exchange both --market both --repeats 3
```

`--repeats 3` выбран под самый строгий лимит (MEXC-фьючерсы — 4 запроса/2с).
Binance/OKX можно гонять отдельно с бóльшим числом повторов.

## Установка

```bash
pip install -r requirements.txt
```

## Ключи API (.env)

Ключи **не хранятся в конфигах** и не коммитятся в git. Берутся из переменных
окружения, которые удобно держать в файле `.env`:

```bash
cp .env.example .env
```

```dotenv
BINANCE_API_KEY=...
BINANCE_API_SECRET=...

OKX_API_KEY=...
OKX_API_SECRET=...
OKX_API_PASSPHRASE=...

MEXC_API_KEY=...
MEXC_API_SECRET=...
```

`.env` подхватывается автоматически (через `python-dotenv`, а без пакета —
встроенным мини-парсером). Реальные переменные окружения имеют приоритет над
`.env` (удобно для CI). Для Binance можно задать отдельные ключи под фьючерсы и
спот (`BINANCE_FUTURES_API_KEY` / `BINANCE_SPOT_API_KEY` и секреты).

**Права у ключей:** торговля (Trade / Order Placing), без вывода средств,
желательно привязка к IP. Для **MEXC-фьючерсов** дополнительно нужен пройденный
**KYC** и включённое право **Futures API trading** (иначе фьючерсные эндпоинты
вернут ошибку прав).

## Запуск

```bash
python3 latency_test.py                       # спросит подтверждение (реальные деньги)
LATENCY_CONFIRM=1 python3 latency_test.py     # без подтверждения (= --yes)
python3 socket_test.py
```

**Рекомендуемый первый боевой прогон** — один рынок, один цикл, чтобы убедиться,
что ордер именно отменяется, а не исполняется:

```bash
LATENCY_CONFIRM=1 python3 latency_test.py --exchange binance --market spot --repeats 1
```

### Флаги командной строки

| Флаг | Назначение |
|---|---|
| `--exchange all\|both\|binance\|okx\|mexc\|binanceus\|bybit\|bitget\|bingx\|coinbase` | Какую биржу гонять. `both` = Binance/OKX/MEXC (как раньше); `all` = все биржи; либо имя одной |
| `--market both\|spot\|futures` | Какой рынок (по умолчанию оба) |
| `--repeats N` | Число повторов цикла (перекрывает `LATENCY_REPEATS`) |
| `--auto-price` | Авто-цена от рынка (по умолчанию включена в обычных конфигах) |
| `--auto-size` | Авто-размер: минимальный валидный объём (по умолчанию включён) |
| `--conditional` | Условный (trigger/algo) бенчмарк — отдельные конфиги (см. ниже) |
| `--no-balance-guard` | Отключить проверку баланса перед условным ордером (не рекомендуется) |
| `--skip-time-check` | Не сверять локальные часы с временем биржи |
| `--yes` / `-y` | Не спрашивать подтверждение |

## Результаты (пример, сервер в Токио)

Обычный лимитный ордер (мин. объём, неисполняемая цена), мс — «Итого повт.»
(среднее place+cancel на прогретом соединении):

| Биржа / рынок | REST | WS |
|---|---|---|
| Binance Futures | ~13 | **~7** |
| Binance Spot | ~21 | **~9** |
| OKX Futures | ~126 | ~109 |
| OKX Spot | ~130 | ~119 |
| MEXC Futures | ~235 | — нет WS |
| MEXC Spot | ~96 | — нет WS |

Условный (trigger) ордер — фьючерсы, только REST:

| Биржа | place (ср.) | cancel (ср.) | итого |
|---|---|---|---|
| Binance | ~14 | ~16 | ~29 |
| MEXC | ~51 | ~26 | ~77 |
| OKX | ~93 | ~76 | ~169 |

Числа зависят от близости сервера к матчинг-движку: Binance USDⓈ-M стоит в
Токио (AWS ap-northeast-1), OKX/MEXC — в Гонконге/Сингапуре. WS обычно быстрее
REST (меньше оверхеда на запрос).

## Авто-цена и авто-размер

В обычных конфигах (`config_*_PROD.json`) `auto_price` и `auto_size` включены по
умолчанию, поэтому плейсхолдеры `price`/`size`/`quantity` там — лишь fallback.

- **Авто-цена** — берёт лучшие бид/аск, отступает на `PRICE_OFFSET` процентов в
  «безопасную» сторону (BUY ниже бида, SELL выше аска) и округляет к шагу цены.
  Ордер заведомо не пересекает спред (не исполнится) и попадает в ценовой коридор
  биржи (price-band фильтры: Binance `PERCENT_PRICE_BY_SIDE`, OKX/MEXC лимит
  цены). Запросы цены — **до** таймера, на замер не влияют.
- **Авто-размер** — берёт с биржи минимальный валидный объём: Binance —
  `exchangeInfo` (`LOT_SIZE`/`NOTIONAL`); OKX — `public/instruments`
  (`minSz`/`lotSz`, для SWAP в **контрактах**); MEXC — спот `exchangeInfo`,
  фьючерс `contract/detail.minVol` (контракты).

Отступ — `PRICE_OFFSET` (проценты, по умолчанию `1.0`). Перед таблицей печатается:

```
  [auto-price] Binance futures: 60934.5
  [auto-size]  Binance futures: 0.001
```

## Условный (trigger/algo) бенчмарк

Условные ордера на всех биржах идут через **отдельную algo-подсистему** с другой
задержкой, чем обычный LIMIT, и **только по REST** (WS-API размещения algo-ордеров
нет ни у одной из бирж). Включается `--conditional`, использует отдельные
конфиги; **только фьючерсы** (на споте условных нет/непрактично).

```bash
LATENCY_CONFIRM=1 python3 latency_test.py --conditional --exchange both --market futures --repeats 3
```

Куда уходит ордер на каждой бирже (BUY с недостижимым триггером — никогда не
срабатывает, маржу до срабатывания не резервирует, сразу отменяется):

| Биржа | Эндпоинт | Примечание |
|---|---|---|
| Binance | `POST /fapi/v1/algoOrder` (`STOP_MARKET`) | С 2025-12-09 условные убраны из `/fapi/v1/order` (даёт `-4120`) и перенесены в Algo-сервис |
| OKX | `POST /api/v5/trade/order-algo` (`ordType=trigger`) | WS-канал `algo-orders` — только подписка на обновления, не размещение |
| MEXC | `POST /api/v1/private/planorder/place/v2` | Фьючерсный API MEXC переоткрыт (перезапуск 31.03.2026); нужен KYC + Futures API trading |

**Balance-guard.** Такая схема безопасна, только пока счёт почти пустой. Поэтому
перед условным ордером скрипт проверяет баланс и **отказывается запускаться**,
если доступно больше `MAX_EQUITY_USDT` (по умолчанию 50 USDT). Если баланс не
прочитать (например, временный сбой query-API) — guard пропускает (fail-open),
т.к. триггер всё равно недостижим. Отключение: `--no-balance-guard` /
`BALANCE_GUARD=0`.

Про размер: **задержка `place`/`cancel` не зависит от объёма** — матчинг-движок
одинаково быстро принимает и минимальный, и крупный несрабатывающий ордер.

## Ограничения транспортов по биржам

- **MEXC / Bitget / BingX / Coinbase** — у этих бирж **нет WS-API размещения
  ордеров** (WS только маркет-дата / user-data; у Coinbase быстрый путь — FIX).
  Замер place/cancel — только REST; WS-ячейки честно помечены ошибкой.
- **WS-торговля есть** у Binance, Binance.US, OKX (лимит) и **Bybit**
  (`wss://stream.bybit.com/v5/trade`, авторизация вне таймера).
- **Условные ордера** — только REST на всех биржах (см. выше).
- **OKX rate-limit (`50011`)** — OKX режет частоту размещения и штрафует за низкий
  fill-ratio (а у бенчмарка он нулевой). Чтобы WS-прогон сразу после REST по
  одному инструменту не упирался в лимит, между REST и WS вставлена пауза
  `TRANSPORT_PAUSE_SEC` (по умолчанию 2 с, вне замера). При жёстком лимите —
  поднимите паузу / снизьте `--repeats`.

## Режим позиции на фьючерсах (hedge / one-way)

В hedge-режиме биржи требуют указывать сторону позиции в каждом ордере. Скрипт
определяет режим счёта автоматически и подставляет нужное поле:

- **Binance** — `GET /fapi/v1/positionSide/dual`; в hedge ставит `positionSide`
  (BUY→LONG, SELL→SHORT), иначе `-4061`.
- **OKX** — `GET /api/v5/account/config`; в `long_short_mode` ставит `posSide`
  (`long`/`short`), иначе `51000`.
- **MEXC** — фьючерсный ордер использует `side` (1 open long / 3 open short).

```
  [pos-mode] Binance futures: hedge → positionSide=LONG
  [pos-mode] OKX futures: long/short → posSide=long
```

## Зачистка после прогона (reconcile)

После каждого рынка скрипт делает **reconcile**: снимает свои незакрытые ордера
(только с префиксом id `lt…`, чужие не трогает) и показывает позицию. Позиции он
**не закрывает** (это была бы новая сделка) — только громко предупреждает:

```
  [reconcile] Binance spot: ок (снято наших=0, осталось=0, позиция=0)
  [reconcile] OKX futures: ⚠ снято наших=0, осталось=0, позиция=0.01 — ПРОВЕРЬТЕ ВРУЧНУЮ!
```

`позиция ≠ 0` — это либо ваша **уже существовавшая** позиция (бенчмарк её не
создавал: `снято наших=0, осталось=0` ⇒ наши ордера не исполнялись), либо
действительно что-то исполнилось — проверьте вручную.

## Проверка и синхронизация времени

Перед прогоном скрипт сверяет локальные часы с временем биржи (Binance
`/fapi/v1/time`, OKX `/api/v5/public/time`, MEXC `/api/v3/time`): при `> 500 мс`
— предупреждение, при `> 3000 мс` — остановка (обойти: `--skip-time-check`).
На Linux синхронизация:

```bash
sudo timedatectl set-ntp true        # или chrony для точности
```

## Конфиги (без секретов)

| Файл | Назначение |
|---|---|
| `config_binance_PROD.json` / `config_okx_PROD.json` / `config_mexc_PROD.json` | Обычный лимитный режим |
| `config_binance_PROD_conditional.json` / `config_okx_PROD_conditional.json` / `config_mexc_PROD_conditional.json` | Условный (trigger) режим |

Пути переопределяются переменными `CONFIG_BINANCE`/`CONFIG_OKX`/`CONFIG_MEXC` и
`CONFIG_*_CONDITIONAL`.

## Переменные окружения (сводка)

| Переменная | Назначение |
|---|---|
| `BINANCE_API_KEY` / `_SECRET` | Ключи Binance (можно раздельные `BINANCE_FUTURES_*` / `BINANCE_SPOT_*`) |
| `OKX_API_KEY` / `_SECRET` / `_PASSPHRASE` | Ключи OKX |
| `MEXC_API_KEY` / `_SECRET` | Ключи MEXC |
| `CONFIG_BINANCE` / `CONFIG_OKX` / `CONFIG_MEXC` | Пути к обычным конфигам |
| `CONFIG_BINANCE_CONDITIONAL` / `CONFIG_OKX_CONDITIONAL` / `CONFIG_MEXC_CONDITIONAL` | Пути к условным конфигам |
| `LATENCY_REPEATS` | Число повторов цикла (по умолчанию 5) |
| `LATENCY_CONFIRM` | `1`/`yes` — пропустить подтверждение (= `--yes`) |
| `AUTO_PRICE` / `PRICE_OFFSET` | Авто-цена и её отступ в % (по умолчанию 1.0) |
| `AUTO_SIZE` | `1`/`yes` — авто-размер (= `--auto-size`) |
| `CONDITIONAL` | `1`/`yes` — условный бенчмарк (= `--conditional`) |
| `BALANCE_GUARD` / `MAX_EQUITY_USDT` | Balance-guard (вкл. по умолчанию) и порог в USDT (50) |
| `TRANSPORT_PAUSE_SEC` | Пауза между REST и WS прогонами рынка (по умолчанию 2 с) |
| `SKIP_TIME_CHECK` | `1`/`yes` — не сверять часы с биржей |
