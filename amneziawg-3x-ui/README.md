# AmneziaWG support for 3x-ui — delivery package

This directory contains a complete implementation that adds **AmneziaWG**
(obfuscated WireGuard) as an inbound protocol to the
[MHSanaei/3x-ui](https://github.com/MHSanaei/3x-ui) panel, so AmneziaWG clients
can connect to it.

## Why it's delivered as a patch (not a PR)

The work targets `MHSanaei/3x-ui`, but this session's GitHub access was scoped to
`alekseygoweb/exchangebenchmark` only — it could not fork `3x-ui`, push to it, or
open a PR there. So the change is delivered here as a git bundle + patch you can
apply to your own clone/fork of 3x-ui. (This branch of ExchangeBenchmark is only
a delivery drop; none of the Go/React code below belongs in this repo.)

The implementation was developed and verified against 3x-ui at upstream commit
`5c725df` (branch `claude/amnesiawg-client-support-rkn6xo`).

## How to apply

Clone or `cd` into your 3x-ui checkout, then either:

**Option A — git bundle (keeps the 3 commits).** Requires your checkout to
contain upstream commit `5c725df` (any recent `main`):

```bash
git fetch /path/to/amneziawg-3x-ui.bundle claude/amnesiawg-client-support-rkn6xo
git checkout -b amneziawg FETCH_HEAD
```

**Option B — combined patch (works on any compatible tree):**

```bash
git checkout -b amneziawg
git apply --3way /path/to/amneziawg-3x-ui.patch
```

Then build as usual (`cd frontend && npm ci && npm run build` to refresh the
embedded `dist/`, then `go build ./...` at the repo root — `make verify` mirrors CI).

## What it does

AmneziaWG's obfuscation is symmetric, so the **server** must speak AmneziaWG too.
3x-ui runs the official Xray-core binary, whose WireGuard inbound is plain
`wireguard-go` with no obfuscation and cannot serve AmneziaWG. This change runs an
**in-process AmneziaWG server** using the
[`amnezia-vpn/amneziawg-go`](https://github.com/amnezia-vpn/amneziawg-go) library
over a userspace gVisor netstack — **no root, no kernel module, no extra binary**,
works in unprivileged Docker — supervised alongside Xray the same way the mtproto
sidecar is.

You get: an **AmneziaWG** inbound protocol in the UI with the obfuscation
parameters (Jc/Jmin/Jmax, S1–S4, H1–H4, I1–I5) and a Randomize button; WireGuard's
per-client management reused verbatim; per-client `.conf`/QR that carry the
matching obfuscation parameters; per-inbound and per-client traffic accounting;
and an egress guard so tunnelled clients can't reach the panel host or its LAN.

See **`amneziawg-feature-doc.md`** in this directory (a copy of the `docs/amneziawg.md`
included in the patch) for the full operator guide, parameter reference, and
verification steps.

## What was changed (summary)

Three commits:

1. `feat(awg): in-process AmneziaWG inbound backend` — the new `internal/awg/`
   package (params + validation, forwarding gVisor netstack, amneziawg-go device
   wrapper, manager) plus integration (protocol enum, Xray-config exclusion,
   runtime dispatch, reconcile+traffic job, port-conflict UDP class); reuses the
   WireGuard client model, so **no DB migration**.
2. `feat(awg): expose obfuscation params to clients page, fence egress, docs` —
   `InboundOption.awgParams`, the netsafe egress guard, and documentation.
3. `feat(awg): AmneziaWG protocol in the web UI` — the React frontend (schema,
   form, defaults, client `.conf` generation), regenerated types, and
   `pages.xray.amneziawg.*` in all 13 i18n locales.

## Verification status

- **Green in this environment:** `go build ./...`, `go vet`, the Go unit tests
  (including a real in-process device lifecycle: start → live peer update →
  teardown), and the full frontend gate (`typecheck`, `lint`, 714 vitest tests,
  `build`).
- **Not exercised here:** a live client handshake + traffic forwarding, which
  needs a real AmneziaWG client and a routable host (unavailable in CI). Follow
  the "Verifying a deployment" section of `amneziawg-feature-doc.md` to confirm
  end-to-end on a server.

## Known limitations (v1)

- AmneziaWG has no share-link URL scheme, so clients are provisioned from the
  panel's config/QR, not the subscription endpoint.
- Client traffic egresses directly from the host and does not pass through Xray's
  routing/outbounds. Routing AmneziaWG through Xray is a possible future step.
- Changing an inbound's key/port/MTU/obfuscation parameters restarts its device
  (brief drop); adding/removing a client is applied live.
