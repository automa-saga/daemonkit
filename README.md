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

## Writing a daemon and a monitor

A **monitor** is a long-running work loop that implements `MonitorRunner`; a
**daemon** runs one or more monitors under the supervisor. Minimal end to end:

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/automa-saga/daemonkit"
)

// 1. A monitor: implement Run + Name.
type PingMonitor struct{ log *slog.Logger }

func (m *PingMonitor) Name() string { return "ping-monitor" }

// Run blocks until ctx is cancelled. Return nil on cancellation (clean
// shutdown); return non-nil ONLY on unexpected failure — that triggers a
// supervised restart with back-off.
func (m *PingMonitor) Run(ctx context.Context) error {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil // NOT ctx.Err()
		case <-t.C:
			m.log.Info("ping")
		}
	}
}

// 2. A daemon: run each monitor under SupervisedMonitor.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	tracker := daemonkit.NewStatusTracker()

	g, ctx := daemonkit.WithContext(ctx)
	for _, m := range []daemonkit.MonitorRunner{&PingMonitor{log: log}} {
		g.Go(m.Name(), func(ctx context.Context) error {
			daemonkit.SupervisedMonitor(ctx, m, daemonkit.SupervisorOptions{
				Tracker:           tracker,
				Logger:            log,
				HeartbeatInterval: 30 * time.Second,
			})
			return nil // SupervisedMonitor returns only on ctx cancellation
		})
	}
	if err := g.Wait(); err != nil {
		log.Error("daemon exited", "error", err)
		os.Exit(1)
	}
}
```

Three contracts the supervisor relies on:

- **`Run` returns `nil` on ctx cancellation**, non-nil only on unexpected failure
  (which triggers a restart). Returning `ctx.Err()` on shutdown causes a spurious
  restart.
- **Never spawn a bare `go func()`** inside a monitor — `recover` is
  per-goroutine, so an unguarded panic there crashes the whole daemon. Use
  `daemonkit.Go` for best-effort background work (its panic is recovered and
  logged), or `daemonkit.WithContext` / `Group` for work whose failure should
  restart the monitor (its panic/error surfaces from `Wait`). Both compose at any
  nesting depth.
- **Guard `ConnectivityError()`** (if you implement it) with an atomic or mutex —
  the status/heartbeat goroutine reads it while `Run` writes.

Optional interfaces add capability without ceremony: `ConnectivityMonitor`
(surface connectivity health in `/status` and heartbeats) and `ProbableMonitor`
(verify prerequisites before `Run` starts). See the
[User Guide](docs/user-guide.md) for probes, control-plane routes, the event
logger, and the file pruner.

## Authoring & auditing with Claude Code

This repo ships a [Claude Code](https://code.claude.com) skill,
[`skills/daemonkit-monitors`](skills/daemonkit-monitors), that encodes the
contracts above so Claude can:

- **write** a new daemon or monitor to the contract, and
- **audit** an existing daemonkit-based repo for the anti-patterns that crash
  daemons or cause data races — bare `go func()`, monitors launched without
  `SupervisedMonitor`, unguarded `ConnectivityError`, raw `errgroup`, and `Run`
  returning non-nil on ctx cancel — and apply the fixes.

Install it into your project, then ask Claude to write or audit:

```bash
mkdir -p .claude/skills
cp -R "$(go env GOMODCACHE)"/github.com/automa-saga/daemonkit@*/skills/daemonkit-monitors .claude/skills/
# then: "write a daemonkit monitor that …" or "audit this repo for daemonkit issues and fix them"
```

See [skills/README.md](skills/README.md) for details.

## Documentation

- [Architecture](docs/architecture.md) — design, the daemon kernel model, and concurrency contracts.
- [User Guide](docs/user-guide.md) — runnable examples for every package.

## License

Apache-2.0. See [LICENSE](LICENSE).
