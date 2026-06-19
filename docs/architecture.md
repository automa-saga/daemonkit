# daemonkit Architecture

`daemonkit` is a small, framework-neutral kernel for building long-running Go
daemons. It bundles three independent packages that are commonly needed
together but can be adopted individually:

| Package | Import | Responsibility |
|---|---|---|
| `daemonkit` | `github.com/automa-saga/daemonkit` | Supervised monitor loop, Unix-socket HTTP control plane, probe framework, `sd_notify` |
| `eventlog` | `github.com/automa-saga/daemonkit/eventlog` | Append-only JSONL milestone/audit logger |
| `filepruner` | `github.com/automa-saga/daemonkit/filepruner` | Strategy-driven retention pruning of files |

## Abstract

A production daemon is rarely just "a `main()` that loops forever". It has to
survive the failure of its own work loops, expose enough state for an operator
to diagnose it without SSH-ing into the host and tailing journald, refuse to
start work for which the host is not actually prepared, and leave a durable
trail of what it did. These concerns are the same whether the daemon is
reconciling a network upgrade, draining a queue, or watching a filesystem —
yet they are routinely re-implemented, slightly differently and slightly
wrongly, in every new daemon.

`daemonkit` factors those concerns out into a reusable kernel. This document
explains the operational problems each component solves and the use cases that
shaped its contract — not just what the API is, but *why it exists*.

## Motivation

Consider a concrete daemon: a host-level agent that watches a Kubernetes
cluster for a "network upgrade" custom resource and, when one appears, runs a
multi-step migration on local disk. It runs unattended on hundreds of hosts,
under `systemd`, with no human watching any individual instance. From that
shape, four hard requirements fall out — and each maps to one part of the kit.

### 1. A work loop that dies must not take the daemon down with it

The upgrade watcher talks to the Kubernetes API. That connection *will* drop —
during an API-server rollout, a network blip, a token refresh. A naive
`go watch()` goroutine that returns on error simply… stops, silently, and the
daemon keeps running as a hollow process that no longer does its job. Nobody
notices until an upgrade is missed.

What's actually needed is a supervisor: restart the loop when it fails, but
back off so a hard-down dependency doesn't become a tight crash-loop hammering
the API server, and *escalate* — make noise — once the loop has clearly stopped
recovering on its own. Distinguishing "crashed" from "asked to shut down" must
be unambiguous, or the daemon will either fail to restart real crashes or
"restart" itself during a clean `SIGTERM`.

→ **`SupervisedMonitor`** (below).

### 2. An operator needs to ask "are you healthy?" without reading logs

When one host out of hundreds misbehaves, the operator's first question is
"what is this daemon doing right now?". Grepping journald per host does not
scale and does not compose with tooling. The daemon needs to *answer* that
question over a stable, machine-readable interface — but one that is not exposed
to the network (this is a host-local agent, not a service), and that requires no
port allocation or auth story.

A Unix domain socket with a tiny HTTP surface (`/health`, `/status`) is exactly
that: local-only by construction (filesystem-permissioned), curl-able by a human,
and parseable by a CLI or a monitoring sidecar. The catch is that "status" has to
reflect *live* internal state — including the case where a monitor goroutine is
technically alive but stuck retrying a broken connection.

→ **`Server`** + **`StatusTracker`** + **`ConnectivityMonitor`** (below).

### 3. The daemon must refuse work the host can't support — early and legibly

The migration writes to `/var/lib/<daemon>`. If that directory is missing,
read-only, owned by the wrong user, or the disk is full, the work will fail —
but it will fail *halfway through*, after partial side effects, with a deep and
cryptic error. Far better to check the preconditions *before* starting and fail
with a precise, operator-actionable reason ("directory X is not writable by
uid Y — run Z"). Checks should run concurrently (a slow check shouldn't serialise
the rest) and the first failure should abort the rest.

→ **Probe framework** (`Probe`, `CompositeProbe`, built-in disk probes) (below).

### 4. There must be a durable, auditable record of what happened

Operational logging (journald) is for debugging and is noisy, rotated, and
ephemeral. But "we began upgrade to v0.75.0 on node-3 at 12:00:00Z and it
completed at 12:04:11Z" is an *audit milestone* — it must survive log rotation,
be machine-readable, and be append-only so it can't be quietly rewritten. And
those audit files must not accumulate forever on a disk that also holds the
node's data.

→ **`eventlog`** + **`filepruner`** (below).

### Design goals that follow

- **Dependency-light.** Production code imports only the Go standard library,
  [`errorx`](https://github.com/joomcode/errorx), and
  [`golang.org/x/sync/errgroup`](https://pkg.go.dev/golang.org/x/sync/errgroup).
  There is no dependency on `k8s.io/...` or any orchestration framework, so the
  kit is safe to embed in any daemon regardless of what that daemon talks to.
- **No host coupling.** The kit never reaches into a consumer's internal models.
  Status payloads are injected as `func() any` and serialised verbatim; probe
  failures surface as a plain `ProbeError` the consumer can re-wrap at its own
  boundary. The kit knows nothing about Kubernetes, upgrades, or your domain.
- **Operator-first.** State transitions and failures are emitted via `log/slog`
  with stable, machine-readable `reason` keys, and exposed over a `/status`
  endpoint so operators can diagnose without reading journald.

## Use cases / user stories

The contracts below were shaped by these stories. They are written from the
operator's and the daemon-author's point of view to keep the design honest.

- **As an operator**, when a daemon's work loop hits a transient dependency
  failure, I want it to recover automatically without a tight crash-loop, so a
  10-second API blip doesn't page me.
- **As an operator**, when a daemon is genuinely stuck (crashing repeatedly), I
  want a loud, distinct signal — not the same line every 5 seconds — so I can
  tell "recovering" from "needs hands".
- **As an operator**, I want to run one command against a host and get a
  structured snapshot of every monitor's state, including connectivity errors
  for loops that are alive-but-failing, so triage doesn't require log spelunking.
- **As an operator**, when a daemon won't start, I want the failure to name the
  exact unmet precondition and the command to fix it, not a stack trace from
  deep inside the work.
- **As an auditor**, I want a tamper-evident, append-only record of lifecycle
  milestones that outlives log rotation and is one JSON object per line.
- **As a host owner**, I want those audit and log files pruned by age with a hard
  cap, and I want a file whose age can't be determined to be *kept*, never
  deleted by accident.
- **As a daemon author**, I want all of the above without taking on a heavy
  framework or a logging-backend dependency, and without the kit reaching into
  my domain models.

## The daemon kernel (`daemonkit`)

A daemon built on the kit is typically composed of:

1. **One or more monitors** — goroutines implementing `MonitorRunner` (a `Run(ctx)`
   loop plus a stable `Name()`), each supervised by `SupervisedMonitor`.
2. **A control-plane `Server`** — an HTTP server bound to a Unix domain socket
   exposing process-level `/health` and `/status`, plus any component-scoped
   routes registered by `ComponentHandler` implementations.
3. **A `StatusTracker`** — a concurrency-safe map of per-monitor state that the
   supervisor updates on every transition and the `/status` handler snapshots.
4. **Optional `sd_notify`** — `NotifyReady` / `NotifyStopping` integrate with
   `systemd` `Type=notify` units, and an opt-in `Watchdog` keepalive (see
   [Watchdog](#optional-systemd-watchdog)).

```
                       daemon process
+--------------------------------------------------------------+
|                                                              |
|  monitor goroutine(s)            +--------------+            |
|  +-----------------+   crash     | Supervised   |  set state |
|  | MonitorRunner   |------------>| Monitor      |---------+  |
|  |  Run(ctx)       |   restart   | (restart +   |         |  |
|  |  Name()         |<------------| backoff loop)|         v  |
|  +-----------------+             +--------------+   +----------+
|                                                     | Status   |
|                                       Snapshot()    | Tracker  |
|                                  +------------------ | (mutex)  |
|                                  v                   +----------+
|  +------------------------------------+                       |
|  |  Server (net/http on unix socket)  |                       |
|  |    GET /health   -> liveness       |                       |
|  |    GET /status   -> StatusFn()     |                       |
|  |    /<component>/ -> ComponentHandler|                      |
|  +------------------------------------+                       |
|                  |                                            |
|  sd_notify: NotifyReady() / NotifyStopping()                 |
+------------------|-------------------------------------------+
                   v
        /run/<daemon>/control.sock  <--  operator / CLI
                                         (curl --unix-socket ...)
```

### Supervised monitor loop

**Why it exists:** to satisfy use cases 1 and 2 — a work loop that recovers from
transient failure on its own, backs off so a hard-down dependency can't become a
crash-loop, and escalates loudly once it's clearly not recovering. Without it,
every daemon re-invents restart-and-backoff, and most get the
crash-vs-clean-shutdown distinction subtly wrong.

`SupervisedMonitor(ctx, runner, opts)` runs a `MonitorRunner` in a restart loop
and never returns an error — it absorbs crashes until `ctx` is cancelled. The
`SupervisorOptions` carry the optional `StatusTracker`, the logger, and the
heartbeat interval (see [Observability](#observability)); the zero value is valid.

- A **non-nil** return from `Run` is a crash → restart after back-off.
- A **nil** return, or a cancelled `ctx`, is a clean exit → no restart.
- Back-off starts at 5 s, doubles on each crash up to a 5 min cap, and resets
  after a monitor runs stably for 60 s before its next crash.
- After 5 consecutive crashes (then 10, 15, …) a `MonitorDegraded` error is
  logged so ops stays alerted while the monitor is unhealthy.

The runner contract: return `nil` on `ctx` cancellation, return non-nil only on
unexpected failure, and be safe to call again after a failure. (This is what lets
the supervisor tell "the operator asked me to stop" from "my dependency broke" —
the single most common bug in hand-rolled supervision.)

```
                  +-----------+
   start -------> |  running  | -----------------------------+
                  +-----------+                              |
                    |     ^                                  | Run returns nil
   Run returns      |     | wait dur, restart                | OR ctx cancelled
   non-nil (crash)  |     |                                  v
                    v     |                            +-----------+
              +------------------+                     |  stopped  | (terminal)
              | backoff:<dur>    |  ctx cancelled ---->+-----------+
              | 5s,10s,20s..<=5m |
              +------------------+
   reset to 5s if the monitor ran stably >= 60s before crashing
   every 5th consecutive crash (5,10,15..) logs a MonitorDegraded error
```

The reset-after-stable rule is what makes "degraded" meaningful: a monitor that
crashes once an hour is recovering fine and should never trip the degraded alert,
while one crashing every few seconds should — and the streak counter, reset by
any stable run, is exactly that signal.

### Status reporting

**Why it exists:** to satisfy use case 2 — let an operator (or a CLI/monitoring
sidecar) read live per-monitor state over the control socket. The subtle part is
the *alive-but-failing* monitor.

`StatusTracker` records each monitor's state (`running`, `backoff:<dur>`,
`stopped`). But a monitor stuck in a watch-retry loop *inside* `Run` is, by the
supervisor's definition, "running" — it hasn't returned an error, so it's not
in back-off. To the operator that monitor is broken, and the naive status would
lie. Monitors that maintain an in-process connectivity error can therefore
implement `ConnectivityMonitor`; its `ConnectivityError()` is overlaid onto the
snapshot so a monitor that is alive but failing still reports the failure via
`/status`. Implementations **must** make `ConnectivityError()` safe for
concurrent read — the HTTP goroutine reads it while the monitor goroutine writes
it.

### Control-plane server

**Why it exists:** to satisfy use case 2's transport requirement — a local-only,
auth-free, port-free interface an operator can both `curl` and script against. A
Unix domain socket is local by construction (no network exposure) and
filesystem-permissioned (no auth code to get wrong), which is exactly right for a
host-local agent.

`NewServer(sockPath, opts, cfg)` builds an `http.Server` over a Unix socket.
Process-level routes `/health` (liveness) and `/status` (full status from the
injected `StatusFn`) are always registered. Each `ComponentHandler` registers
its own `/<component>/...` route sub-tree; process routes must not be claimed by
a component. `Start(ctx)` removes any stale socket, listens, `chmod`s the socket
to `0660`, serves, and shuts down cleanly on `ctx` cancellation. The `StatusFn`
indirection is what keeps the kit decoupled from your domain: it serialises
whatever you return verbatim and never needs to know its shape.

### Probe framework

**Why it exists:** to satisfy use cases 3 — fail *before* doing partial work,
with a precise, operator-actionable reason, and check preconditions concurrently.
A component that needs three disk preconditions shouldn't check them serially,
and the first failure shouldn't be masked by a slow sibling check.

Prerequisite checks implement the leaf `Probe` interface (`Probe(ctx) error`).
`CompositeProbe` fans out to its leaves concurrently via `errgroup`; the first
failure cancels its siblings. Composites satisfy `Probe`, so they nest to any
depth. `BuildComponentProbe` collects `RequiredProbe()` from every monitor that
implements `ProbableMonitor` and returns a single component-level probe (or
`nil` when nothing is required) — so a component's preconditions are *declared by
its monitors* rather than wired up by hand. Built-in leaf probes for common host
checks: `DiskPermissionProbe`, `DiskWriteTestProbe`, `DiskOwnershipProbe`.
`TaggedProbe` wraps any probe to attach a stable `Reason`/`Resolution` to its
failure — which is what turns "permission denied" into "directory X not writable
by uid Y — run Z".

```
BuildComponentProbe("my-component", monitors)
       |  collects RequiredProbe() from each ProbableMonitor
       v
  CompositeProbe("my-component")     Probe(ctx): errgroup fan-out;
       |                             first failure cancels siblings
       +-- DiskPermissionProbe          (leaf)
       +-- DiskWriteTestProbe           (leaf)
       +-- CompositeProbe (nested) -----+-- TaggedProbe --> leaf
                                        +-- ... leaf
```

Because a `CompositeProbe` is itself a `Probe`, the tree nests to any depth and
every level fans out concurrently.

## Logging

daemonkit logs through the standard library's `log/slog` and stays
logging-backend agnostic — it has no logging dependency of its own. Only the
`daemonkit` package logs (the supervised-monitor loop and the control-plane
server); `eventlog` and `filepruner` never log, they return `errorx` errors to
the caller.

Attach a `*slog.Logger` for the kit's own diagnostics (monitor
crash/back-off/degradation, server lifecycle): pass it as `ServerOptions.Logger`
to `NewServer`, and as `SupervisorOptions.Logger` to `SupervisedMonitor`.

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

srv := daemonkit.NewServer(sock,
    daemonkit.ServerOptions{Logger: logger},
    daemonkit.ServerConfig{})

go daemonkit.SupervisedMonitor(ctx, mon,
    daemonkit.SupervisorOptions{Tracker: tracker, Logger: logger})
```

If no logger is provided, daemonkit stays silent (it uses `slog.DiscardHandler`)
— it never writes to the global `slog` default implicitly.

We recommend [`logx`](https://github.com/automa-saga/logx) so you get `zerolog` +
`lumberjack` (console writer, rolling files, level, etc.) while still driving
daemonkit through the `slog` API:

```go
logx.Initialize(logx.LoggingConfig{
    Level: "info", ConsoleLogging: true,
    FileLogging: true, Directory: "/var/log/myapp", Filename: "daemon.log",
    MaxSize: 50, MaxBackups: 3, MaxAge: 30, Compress: true,
})

logger := slog.New(logx.NewSlogHandler())

srv := daemonkit.NewServer(sock,
    daemonkit.ServerOptions{Logger: logger},
    daemonkit.ServerConfig{})

go daemonkit.SupervisedMonitor(ctx, mon,
    daemonkit.SupervisorOptions{Tracker: tracker, Logger: logger})
```

## Observability

The two requirements above — operational logging (use case 2) and a durable
audit trail (use case 4) — are both *local* by default: `slog` output goes to a
console/file, the audit JSONL lands on disk, and `/health` + `/status` live
behind a host-local Unix socket. On a fleet of unattended hosts that is not
enough — an operator should be able to ask "is node-3's daemon healthy?" from a
central place without SSH-ing to the host.

daemonkit makes the daemon *remotely* observable without taking on any
telemetry dependency itself, by keeping both signal streams in standard,
collector-friendly formats and letting a local agent ship them:

- **Logs → OTLP.** Because daemonkit logs through `log/slog`, routing it via
  [`logx`](https://github.com/automa-saga/logx) produces structured records with
  stable `reason` keys (`MonitorCrash`, `MonitorDegraded`, `ServerStarted`, …).
  A local collector — [Grafana Alloy](https://grafana.com/docs/alloy/) — tails the
  daemon's log file (or journald unit) and exports those records over **OTLP** to
  a remote logs backend (Loki/Tempo/an OTLP gateway). The `reason` keys become
  queryable labels, so "show me every host that logged `MonitorDegraded` in the
  last hour" is a single fleet-wide query.
- **Audit events → OTLP.** The `eventlog` JSONL files are one self-describing
  JSON object per line by design, so the same Alloy instance can tail them and
  export the audit milestones (`UpgradeStarted`, `UpgradeCompleted`, …) alongside
  the operational logs — keeping the on-host audit file authoritative while still
  surfacing milestones centrally.

```
        host (one of many)                         central
+-------------------------------------+      +----------------------+
|  daemon (daemonkit)                 |      |                      |
|    slog ──► /var/log/app/daemon.log |      |   OTLP gateway       |
|    eventlog ──► /var/log/app/*.jsonl|      |   (Loki / Tempo /    |
|                   |                 |      |    OTel collector)   |
|                   v                 | OTLP |                      |
|              Grafana Alloy  ────────────────►   dashboards,       |
|              (tail + export)        |      |   fleet-wide alerts  |
+-------------------------------------+      +----------------------+
```

The kit's contribution to this is deliberate, not incidental: stable
machine-readable `reason` keys, a backend-agnostic `slog` seam, and an
append-only one-object-per-line audit format are exactly what a collector needs.
daemonkit never speaks OTLP itself — that keeps the production dependency surface
at stdlib + `errorx` + `errgroup` and leaves the transport choice (Alloy, a raw
OTel collector, vendor agent) to the operator.

### Heartbeats

A pull-based `/health` and transition-only logs share one blind spot: a monitor
that is **alive but wedged** — blocked forever inside `Run`, never crashing,
never logging — emits no signal at all, and remotely "silent because healthy" is
indistinguishable from "silent because dead". `SupervisedMonitor` therefore can
emit a periodic **heartbeat** while a monitor is in the `running` state: a
`log.Info` record with `reason=MonitorHeartbeat`, the monitor name, and its
current uptime. Exported via the logs→OTLP path above, the heartbeat turns
*absence of signal* into an alertable condition — a backend can fire when it has
seen no `MonitorHeartbeat` from a given host/monitor within N intervals, catching
the wedged-but-alive case that liveness alone misses.

The heartbeat carries **health, not just liveness.** When the monitor implements
`ConnectivityMonitor`, each heartbeat includes `healthy=true|false` and — when
failing — `connectivity_reason`/`connectivity_error`. So an *alive-but-failing*
monitor (e.g. one stuck retrying a broken watch, which the supervisor still
considers "running") reports its failure in the push stream too, without a
`/status` scrape. A backend can therefore alert on either *missing* heartbeats
(dead/wedged) **or** `healthy=false` (alive but failing). Monitors that can't
observe their own health simply omit the field.

The heartbeat is interval-driven and independent of the work loop, so a monitor
that legitimately blocks for a long time (e.g. a long `watch`) still heartbeats.
It is configured via `SupervisorOptions.HeartbeatInterval` and is **disabled by
default** (zero interval) — in keeping with the kit's silent-by-default posture,
a consumer opts in to the extra log/OTLP volume:

```go
go daemonkit.SupervisedMonitor(ctx, mon, daemonkit.SupervisorOptions{
    Tracker:           tracker,
    Logger:            logger,
    HeartbeatInterval: 30 * time.Second, // 0 (default) disables heartbeats
})
```

### Optional systemd watchdog

The heartbeat makes a wedged monitor *observable*; the systemd **watchdog** lets
systemd *act* on a wedged process automatically. With `WatchdogSec=` set in the
unit, systemd expects the daemon to send `WATCHDOG=1` to `NOTIFY_SOCKET` on an
interval; if it stops arriving, systemd kills and (with `Restart=on-failure`)
restarts the process. It is the last-resort backstop for the one failure neither
the supervisor nor the heartbeat can self-recover from — a *total process freeze*
(both of those loops are frozen too).

`Watchdog(ctx, opts)` runs that keepalive. It is **fully opt-in**: nothing else
in the kit calls it, and it is a no-op (returns immediately) unless the unit
enabled the watchdog — so it is always safe to call. Pair it with
`Restart=on-failure`:

```ini
# unit file
[Service]
Type=notify
WatchdogSec=30s
Restart=on-failure
```

```go
go daemonkit.Watchdog(ctx, daemonkit.WatchdogOptions{Logger: logger})
```

By default the keepalive is **unconditional** — it proves only that the process
is responsive, guarding against a hard freeze. That is the right setting for a
multi-monitor daemon, because withholding the ping bounces the *whole* process
and would reset every healthy monitor — directly against the "running but
degraded is good" model below.

For a **single-monitor** daemon the calculus flips: the process *is* the
monitor, so a restart has no healthy-monitor collateral, and you can gate the
keepalive on liveness via `WatchdogOptions.IsAlive`. When `IsAlive()` returns
false the ping is withheld and systemd restarts the process — turning the
watchdog into an automatic recovery path for that single monitor:

```go
go daemonkit.Watchdog(ctx, daemonkit.WatchdogOptions{
    Logger:  logger,
    IsAlive: func() bool { return mon.Healthy() }, // single-monitor daemons only
})
```

The watchdog deliberately does **not** react to per-monitor degradation in a
multi-monitor daemon — that is owned by the supervisor, `ConnectivityError`, and
the heartbeat. It exists solely to recover a frozen process.

> **Recovering from a control-plane deadlock (not just a total freeze).** An
> unconditional keepalive only catches a *whole-runtime* freeze — an independent
> ticker keeps pinging even if the HTTP server goroutine or the `StatusTracker`
> lock has deadlocked while the Go scheduler still runs, so systemd never
> restarts. To catch that, gate `IsAlive` on a self-request that exercises the
> machinery you want to protect — `/status` runs through the serve loop, the
> `StatusFn`, and the tracker lock:
>
> ```go
> IsAlive: func() bool {
>     // selfGet dials the control socket and returns the HTTP status code.
>     return selfGet(sockPath, "/status", 2*time.Second) == http.StatusOK
> }
> ```
>
> Two caveats make this consumer-owned rather than a kit default: it couples
> liveness to your `StatusFn` latency, and too tight a timeout will false-restart
> a healthy-but-busy daemon — so **size the timeout generously against your own
> `/status` latency**. Use `/status`, not `/health` (the latter is a static 200
> that never touches the lock). For a true deadlock this is safe even in a
> multi-monitor daemon: if shared machinery is wedged, no monitor is healthy
> anyway, so the restart has no healthy-monitor collateral.

## Event log (`eventlog`)

**Why it exists:** to satisfy use case 4 — a durable, machine-readable audit
trail of lifecycle milestones, distinct from operational logging. journald is the
wrong place for "this upgrade started/finished": it's rotated away, interleaved
with debug noise, and not designed to be authoritative.

An append-only JSONL writer for sparse lifecycle milestones (an audit trail —
not operational logging, which belongs in journald). `NewOperation(dir, id)`
truncates a fresh per-operation file; `NewAppend(dir, name)` appends across
opens. Each `Log(Event)` writes one line and `fsync`s, so a crash can't lose an
acknowledged milestone. All `Event` fields are required and validated (a
half-populated audit record is worse than none); only `INFO` and `ERROR` levels
exist by design. Concurrent writes from multiple goroutines produce well-formed
lines.

## File pruner (`filepruner`)

**Why it exists:** to satisfy the host-owner story — those audit files and rolled
logs accumulate on a disk that also holds node data, so they must be pruned by
age with a hard cap. The dangerous failure mode is deleting the wrong thing, so
the design is biased toward *keeping* files when in doubt.

`New(strategy).Prune(dir, glob, keep)` enforces retention in two passes: first
it removes files the `Strategy` marks prunable, then it enforces a hard cap of
`keep` files (oldest-by-name first). A file whose eligibility cannot be
determined (the strategy returns an error) is **protected** — never removed by
either pass. Built-in strategies: `FilenameTimestampStrategy` (age from a
timestamp embedded in the filename), `ModTimeStrategy` (age from mtime),
`FileSizeStrategy` (size threshold). Combine them with `All(...)` / `Any(...)`.

## Error handling

Each package exposes an `errorx` namespace (`daemonkit`, `eventlog`,
`filepruner`). Probe failures surface as `daemonkit.ProbeError`, a plain struct
carrying a stable machine-readable `Reason` and an optional `Resolution`.
Consumers re-wrap it into their own `errorx` namespace at the boundary when they
want richer styling — the kit stays free of consumer-specific model coupling.

## Prior art & alternatives

daemonkit does not invent new algorithms — each concern it covers has
established prior art in the Go ecosystem. It is an opinionated, dependency-light
bundle for one specific operational shape: a host-local, `systemd`-managed,
unattended fleet agent with a Unix-socket control plane. Naming the alternatives
honestly is part of choosing it deliberately.

| Concern | daemonkit | Established alternative |
|---|---|---|
| Supervised restart loop | `SupervisedMonitor` | [`thejerf/suture`](https://github.com/thejerf/suture), [`oklog/run`](https://github.com/oklog/run) |
| `sd_notify` + watchdog | `NotifyReady` / `NotifyStopping` / `Watchdog` | [`coreos/go-systemd`](https://github.com/coreos/go-systemd) |
| Health/status HTTP | `Server` + `/health` + `/status` | [`alexliesenfeld/health`](https://github.com/alexliesenfeld/health) and similar |
| File retention | `filepruner` | [`lumberjack`](https://github.com/natefinch/lumberjack) (rolling), `logrotate` |

### Why not `suture`?

`suture` is the closest, most mature competitor for the supervision piece — and
for a daemon with **interdependent** monitors that need supervisor *trees* or
restart *strategies* (one-for-one, one-for-all, rest-for-one), it is the better
choice. daemonkit deliberately does **not** adopt it, for three reasons grounded
in how these daemons actually run:

1. **The per-monitor loop does domain-specific recovery that a generic
   supervisor can't.** In practice each monitor is written to *self-heal inside
   its own `Run` loop* rather than crash out to the supervisor: it retries with
   its own back-off, and on failure it performs recovery a restart could never
   do — e.g. rebuilding a client on an auth error, setting a *distinct*
   connectivity reason per failure mode, and clearing that error within one watch
   cycle. A generic "restart the goroutine" supervisor (daemonkit's *or*
   suture's) sits dormant for such monitors, so swapping one for the other buys
   nothing — the recovery that matters lives in the monitor, by design.

2. **Fewer third-party dependencies means the daemon can run in production for
   years without forced upgrades.** Every dependency is a future CVE advisory, a
   breaking-change migration, a supply-chain surface. A host agent that must run
   untouched on hundreds of machines for years is best served by the smallest
   possible dependency set — here, stdlib + `errorx` + `errgroup`. Pulling in a
   supervision framework to use ~10% of its surface trades that durability for
   convenience daemonkit doesn't need.

3. **"Running but degraded" is the right resilience model for a long-lived
   agent.** Rather than crash-and-restart, a monitor stays *running* and reports
   itself *degraded* (via `ConnectivityError` and the health-carrying heartbeat)
   while it keeps retrying. This is what lets the daemon ride out multi-hour
   dependency outages and self-heal the moment the dependency returns — without a
   process bounce that would reset every other healthy monitor. The supervisor is
   the backstop for the unexpected (a panic, an unhandled return); the monitor's
   own loop is the primary, deliberate resilience path.

Reach for `suture` if you grow interdependent monitors needing restart
strategies or nested supervision. For independent, self-healing watch loops on a
long-lived host agent, the hand-rolled `SupervisedMonitor` plus a dependency-light
surface is the deliberate, better-fitting choice.

### Why not `coreos/go-systemd`?

`go-systemd`'s `daemon` subpackage is the canonical `sd_notify`/watchdog
implementation, and it is itself stdlib-only — so the dependency cost would be
low. The kit hand-rolls the equivalent (`NotifyReady`/`NotifyStopping`/`Watchdog`
in ~120 lines of `net`+`os`) anyway, for the same reason as point 2 above: a host
agent that must run untouched for years is best served by the smallest possible
dependency set, and `READY=1`/`STOPPING=1`/`WATCHDOG=1` over `NOTIFY_SOCKET` is a
stable, trivially-reimplemented protocol that does not warrant a module
requirement. Adopt `go-systemd` if you need its richer surface (journal, D-Bus,
socket activation); for just the notify + watchdog states, the in-kit version is
the deliberate choice.

## See also

- [User Guide](user-guide.md) — runnable usage examples for each package.
