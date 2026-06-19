# daemonkit User Guide

Runnable examples for each package. All snippets compile against the public
API; adjust paths and identifiers for your daemon.

## Install

```bash
go get github.com/automa-saga/daemonkit@latest
```

Requires Go 1.26+.

## A minimal daemon

Wire a monitor, a supervisor, a status tracker, and the control-plane server
together:

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/automa-saga/daemonkit"
)

// myMonitor implements daemonkit.MonitorRunner.
type myMonitor struct{}

func (m *myMonitor) Name() string { return "my-monitor" }

func (m *myMonitor) Run(ctx context.Context) error {
	// Do real work here, blocking until ctx is cancelled. Return nil on clean
	// shutdown; return a non-nil error to trigger a supervised restart.
	<-ctx.Done()
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Optional: inject a logger. When omitted, daemonkit stays silent.
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	tracker := daemonkit.NewStatusTracker()
	go daemonkit.SupervisedMonitor(ctx, &myMonitor{}, daemonkit.SupervisorOptions{
		Tracker: tracker,
		Logger:  logger,
		// HeartbeatInterval: 30 * time.Second, // opt in to periodic heartbeats
	})

	srv := daemonkit.NewServer(
		"/run/mydaemon/control.sock",
		daemonkit.ServerOptions{
			// Serialised verbatim for GET /status.
			StatusFn: func() any { return tracker.Snapshot() },
			Logger:   logger,
		},
		daemonkit.ServerConfig{},
	)

	_ = daemonkit.NotifyReady() // systemd Type=notify
	defer func() { _ = daemonkit.NotifyStopping() }()

	// Optional: with WatchdogSec= set in the unit, keep systemd's watchdog fed.
	// No-op if the unit didn't enable it, so it's always safe to call.
	go daemonkit.Watchdog(ctx, daemonkit.WatchdogOptions{Logger: logger})

	if err := srv.Start(ctx); err != nil {
		panic(err)
	}
}
```

> **Logging:** daemonkit logs through `log/slog` and is backend-agnostic. Pass a
> `*slog.Logger` to `SupervisedMonitor` and via `ServerOptions.Logger`; omit them
> and the kit stays silent (`slog.DiscardHandler`). See the
> [Architecture › Logging](architecture.md#logging) section for the recommended
> `logx` (zerolog + lumberjack) setup.

Query it over the socket:

```bash
curl --unix-socket /run/mydaemon/control.sock http://localhost/health
curl --unix-socket /run/mydaemon/control.sock http://localhost/status
```

## Component routes

Register a component's own route sub-tree under its `/<component>/` prefix:

```go
import "net/http"

type myComponent struct{}

func (c *myComponent) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /my_component/info", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
}

// Pass it in:
// daemonkit.ServerOptions{ComponentHandlers: []daemonkit.ComponentHandler{&myComponent{}}}
```

## Probes

Verify prerequisites before a component starts; sub-probes run concurrently and
the first failure cancels the rest:

```go
probe := daemonkit.NewCompositeProbe("my-component",
	&daemonkit.DiskWriteTestProbe{Dir: "/var/lib/mydaemon"},
	&daemonkit.DiskPermissionProbe{Path: "/var/lib/mydaemon", Permission: 0o750},
)

if err := probe.Probe(ctx); err != nil {
	// One or more prerequisites are not satisfied.
}
```

## Event log

Append-only JSONL milestones for an audit trail:

```go
import (
	"time"

	"github.com/automa-saga/daemonkit/eventlog"
)

logger, err := eventlog.NewOperation("/var/log/mydaemon", "upgrade-20260619T120000Z")
if err != nil {
	// handle
}
defer logger.Close()

err = logger.Log(eventlog.Event{
	Ts:          time.Now().UTC(),
	Level:       eventlog.LevelInfo,
	Reason:      "UpgradeStarted",
	Msg:         "begin upgrade to v0.75.0",
	OperationID: "upgrade-20260619T120000Z",
	NodeID:      "node-3",
})
```

All `Event` fields are required — `Log` rejects an event with any zero value.
Use `NewAppend(dir, name)` instead of `NewOperation` to append across process
restarts rather than truncating.

## File pruner

Retain recent files and prune the rest by age, with a hard cap:

```go
import (
	"time"

	"github.com/automa-saga/daemonkit/filepruner"
)

pruner := filepruner.New(filepruner.FilenameTimestampStrategy{
	Layout: "20060102T150405Z",
	MaxAge: 30 * 24 * time.Hour,
})

// Remove matching files older than 30 days, then cap to the 50 newest.
if err := pruner.Prune("/var/log/mydaemon", "upgrade-*.jsonl", 50); err != nil {
	// handle
}
```

Combine strategies with `filepruner.All(...)` (prune only when every strategy
agrees) or `filepruner.Any(...)` (prune when any does). A file whose eligibility
can't be determined is never deleted.

## See also

- [Architecture](architecture.md) — the design and concurrency contracts.
