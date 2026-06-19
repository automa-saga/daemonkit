# daemonkit

[![PR Checks](https://github.com/automa-saga/daemonkit/actions/workflows/flow-pull-request-checks.yaml/badge.svg)](https://github.com/automa-saga/daemonkit/actions/workflows/flow-pull-request-checks.yaml)

Every production daemon eventually needs the same things: work loops that
survive their own failures, a way for operators to ask "are you healthy?"
without SSH-ing into the host, a refusal to start work the host can't support,
and a durable record of what it did. These get re-implemented — slightly
differently and slightly wrongly — in every new daemon.

**daemonkit** is a small, framework-neutral kernel that factors them out, so you
can stand up a resilient, observable Go daemon without taking on a heavy
framework or a sprawling dependency tree.

The module deliberately keeps a minimal production dependency surface:

- [`github.com/joomcode/errorx`](https://github.com/joomcode/errorx) — typed errors
- [`golang.org/x/sync/errgroup`](https://pkg.go.dev/golang.org/x/sync/errgroup) — goroutine groups
- the Go standard library (`log/slog`, `net/http`, `encoding/json`, ...)

## Highlights

- **Supervised monitors** — restart loop with exponential back-off, stable-run
  reset, and a degradation alert after repeated crashes; clean shutdown vs.
  crash is unambiguous.
- **Operator-first control plane** — `/health` and `/status` over a Unix domain
  socket (local-only, no port or auth), plus component-scoped routes.
- **Health-carrying heartbeat** — opt-in periodic records that report liveness
  *and* connectivity health, so remote observability can alert on the *absence*
  of a signal — catching the alive-but-wedged case liveness alone misses.
- **Probe framework** — concurrent prerequisite checks that fail before partial
  work with a stable, operator-actionable reason and resolution.
- **systemd integration** — `sd_notify` (`READY`/`STOPPING`) plus an opt-in
  watchdog keepalive, with no `go-systemd` dependency.
- **Audit & retention** — fsync'd, append-only JSONL milestones and
  strategy-driven file retention that protects files whose eligibility is
  uncertain.
- **Backend-agnostic logging** — drives `log/slog`, silent by default, pluggable
  into any handler and exportable to OTLP via a local collector.

## Packages

| Import                                        | Purpose                                                                   |
|-----------------------------------------------|---------------------------------------------------------------------------|
| `github.com/automa-saga/daemonkit`            | Control-plane HTTP server, supervised monitor + heartbeat, probe framework, sd_notify + watchdog |
| `github.com/automa-saga/daemonkit/eventlog`   | Append-only JSONL structured event logger                                 |
| `github.com/automa-saga/daemonkit/filepruner` | Retention-based file pruning                                              |

The three packages are mutually independent — import only what you need.

## Install

```bash
go get github.com/automa-saga/daemonkit@latest
```

Requires Go 1.26 or newer.

## Quick start

See the [User Guide](docs/user-guide.md) for probes, component routes, the event logger, and the file pruner.

## Documentation

- [Architecture](docs/architecture.md) — design, the daemon kernel model, and concurrency contracts.
- [User Guide](docs/user-guide.md) — runnable examples for every package.

## License

Apache-2.0. See [LICENSE](LICENSE).
