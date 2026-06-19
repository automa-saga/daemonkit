# daemonkit

[![PR Checks](https://github.com/automa-saga/daemonkit/actions/workflows/flow-pull-request-checks.yaml/badge.svg)](https://github.com/automa-saga/daemonkit/actions/workflows/flow-pull-request-checks.yaml)

Small, dependency-light building blocks for long-running Go daemons: a
supervised monitor restart loop, a Unix-socket control-plane HTTP server with
health/status routes, a composite probe framework, systemd `sd_notify` helpers,
an append-only JSONL event logger, and a retention-based file pruner.

The module deliberately keeps a minimal production dependency surface:

- [`github.com/joomcode/errorx`](https://github.com/joomcode/errorx) — typed errors
- [`golang.org/x/sync/errgroup`](https://pkg.go.dev/golang.org/x/sync/errgroup) — goroutine groups
- the Go standard library (`log/slog`, `net/http`, `encoding/json`, ...)

## Packages

| Import                                        | Purpose                                                                   |
|-----------------------------------------------|---------------------------------------------------------------------------|
| `github.com/automa-saga/daemonkit`            | Control-plane HTTP server, supervised monitor, probe framework, sd_notify |
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
