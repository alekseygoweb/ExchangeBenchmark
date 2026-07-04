# AmneziaWG inbounds

AmneziaWG (AWG) is WireGuard with added packet obfuscation designed to defeat
Deep Packet Inspection: random junk packets before the handshake, per-message
padding, and randomized "magic headers" that hide WireGuard's fixed message-type
bytes. Because the obfuscation is symmetric, **the server must speak AmneziaWG
too** — a stock WireGuard server cannot serve AmneziaWG clients (and vice-versa)
once the obfuscation knobs are non-trivial.

3x-ui runs the official Xray-core binary, whose WireGuard inbound is plain
`wireguard-go` with no obfuscation. So AmneziaWG cannot be served by Xray. This
panel therefore runs an **in-process AmneziaWG server** using the
[`amneziawg-go`](https://github.com/amnezia-vpn/amneziawg-go) library over a
userspace gVisor netstack — the same technique Xray-core uses for its own
WireGuard inbound.

## How it works

- **No root, no kernel module, no extra binary.** The AmneziaWG device runs
  inside the `x-ui` process on a userspace gVisor network stack. It binds the
  inbound's UDP port, terminates the encrypted tunnel, and a TCP/UDP forwarder
  carries the decrypted client traffic out through the host. This works in an
  unprivileged Docker container, unlike the kernel `amneziawg` module.
- **Managed like the mtproto sidecar.** AmneziaWG inbounds are excluded from the
  Xray config (`internal/web/service/xray.go`). A manager
  (`internal/awg/manager.go`) keeps one device per inbound; a cron job
  (`internal/web/job/awg_job.go`, every 10s) reconciles the running devices
  against the enabled `amneziawg` inbounds in the database and folds each
  device's per-peer traffic into normal inbound/client accounting.
- **Same client model as WireGuard.** Each AmneziaWG client is a WireGuard peer
  (keypair + tunnel address + optional preshared key + keepalive). Client CRUD,
  key generation, and address allocation are shared with WireGuard, so no
  database migration is required — the obfuscation parameters live in the
  inbound's settings JSON.
- **Egress is fenced.** The forwarder refuses to dial loopback, private,
  link-local, and unspecified addresses (`internal/util/netsafe`), so a tunnelled
  client cannot reach the panel host's own services or the server's LAN. Clients
  are isolated from each other by the same rule.

## Obfuscation parameters

Set on the inbound; the panel writes matching values into every client `.conf`.
The AmneziaWG 2.0 scheme (as implemented by `amneziawg-go`) is supported:

| Param | Type | Meaning |
|---|---|---|
| `Jc` | int | Junk packet count sent before each handshake (0 disables) |
| `Jmin` / `Jmax` | int | Min/max size of each junk packet (bytes) |
| `S1` / `S2` | int | Extra padding prepended to handshake init / response |
| `S3` / `S4` | int | Padding for cookie-reply / transport messages (2.0) |
| `H1`–`H4` | uint32 or `x-y` range | Magic headers for init / response / cookie / transport; replace WireGuard's fixed 1/2/3/4. Must be four distinct values |
| `I1`–`I5` | string | Optional custom signature ("CPS") junk packets, e.g. `<b 0x…>`, `<r 40>` |

`H1`–`H4` and `S1`–`S4` (and any `I`-packets) **must be identical** on client and
server; `Jc`/`Jmin`/`Jmax` are one-way (client) junk. Leaving all of them unset
makes the device behave as vanilla WireGuard. The inbound form's **Randomize**
button generates a sound obfuscating set.

## Using it

1. Create an inbound, choose protocol **AmneziaWG**, pick a UDP port. A server
   key is generated; click **Randomize** to fill the obfuscation parameters.
2. Add clients exactly like WireGuard clients (a keypair and tunnel address are
   allocated automatically).
3. Open a client's **config / QR** and import the `.conf` into an AmneziaWG
   client app (the standalone AmneziaWG app, or AmneziaVPN). AmneziaWG has no
   `awg://` share-link scheme, so distribution is by `.conf` file or its QR.

## Limitations

- **Not delivered via subscription links.** AmneziaWG has no standard share-link
  URL format, so clients are provisioned from the panel's config/QR, not the
  subscription endpoint.
- **Direct host egress.** Client traffic egresses from the panel host and does
  **not** pass through Xray's routing/outbounds/blocking. Routing AmneziaWG
  through Xray is a possible future enhancement.
- **Parameter edits restart the device.** Changing the server key, port, MTU, or
  obfuscation parameters rebuilds the device and briefly drops sessions;
  adding/removing a client is applied live without a restart.

## Verifying a deployment

The panel's unit tests cover the parameter model, UAPI/`.conf` generation, and a
real device lifecycle (`internal/awg`), but a full client handshake needs a real
AmneziaWG client and a routable host, which CI does not have. To verify a live
deployment:

1. Create an AmneziaWG inbound with randomized parameters and add a client.
2. Import the client `.conf` into an AmneziaWG client (e.g. the AmneziaWG app or
   [`amneziawg-tools`](https://github.com/amnezia-vpn/amneziawg-tools) `awg-quick`).
3. Confirm the handshake completes and traffic flows (`curl https://ifconfig.me`
   should show the server's IP), and that the client's traffic counter advances
   on the panel within ~10s.
4. Confirm a stock WireGuard client with the same keys but **no** obfuscation
   parameters cannot connect (proving the obfuscation is active).
