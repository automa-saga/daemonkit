// SPDX-License-Identifier: Apache-2.0

// Package daemonkit provides a reusable kernel for long-running daemons: a
// supervised-monitor restart loop, a Unix-socket HTTP control plane, and
// sd_notify integration. It depends only on the standard library, errorx, and
// golang.org/x/sync/errgroup, and intentionally imports nothing under
// internal/... or cmd/... so it can be shared across daemons (e.g. the
// solo-provisioner daemon and a future solo-operator daemon).
package daemonkit

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Back-off and degradation parameters for SupervisedMonitor. Declared as
// package-level vars (not consts) so unit tests can override them without
// sleeping for real durations.
var (
	supervisedBackoffInitial  = 5 * time.Second
	supervisedBackoffCap      = 5 * time.Minute
	supervisedStableThreshold = 60 * time.Second

	// supervisedDegradedThreshold is the number of consecutive crashes (without
	// an intervening stable run) before a MonitorDegraded event is emitted.
	// Fires again on every subsequent multiple of this value (5, 10, 15, …) so
	// ops keeps seeing the alert as long as the monitor stays degraded.
	supervisedDegradedThreshold = 5
)

const supervisedBackoffMultiplier = 2.0

// discardLogger is the shared no-op logger used when a caller does not inject
// one. slog.DiscardHandler drops every record, so the kit stays silent and
// allocation-free until a real logger is supplied.
var discardLogger = slog.New(slog.DiscardHandler)

// loggerOrDiscard returns l, or the no-op discardLogger when l is nil, so call
// sites never have to nil-check an optional injected logger.
func loggerOrDiscard(l *slog.Logger) *slog.Logger {
	if l == nil {
		return discardLogger
	}
	return l
}

// StatusError is a rich, operator-facing error descriptor used in /status for
// both monitor connectivity failures and component probe (disk prerequisite)
// failures. Every populated field gives the operator enough context to act
// without opening journalctl.
type StatusError struct {
	// Reason is a stable, machine-readable key matching the log reason field
	// (e.g. "UpgradeMonitorListError", "UpgradeDirOwnershipCheckFailed").
	Reason string `json:"reason"`

	// Message is the human-readable error string.
	Message string `json:"message"`

	// Resolution is an actionable command or instruction the operator should
	// run to resolve the issue. Empty when no specific remediation is known.
	Resolution string `json:"resolution,omitempty"`

	// Since is the RFC 3339 timestamp of when this error was first observed.
	Since string `json:"since"`
}

// MonitorState describes the runtime state of a single supervised monitor.
// State values:
//   - "running"         — monitor is executing normally
//   - "degraded"        — monitor is running but its last operation failed;
//     see Error for details; the monitor continues retrying automatically
//   - "backoff:<dur>"   — monitor crashed (Run returned non-nil) and is
//     waiting before restart
//   - "stopped"         — monitor exited cleanly (ctx cancelled or nil return)
type MonitorState struct {
	State string       `json:"state"`
	Error *StatusError `json:"error,omitempty"`
}

// ConnectivityMonitor is optionally implemented by monitors that maintain an
// in-process record of their last connectivity error (e.g. a K8s watch failure).
// statusSnapshot overlays ConnectivityError onto the tracker state so failures
// are visible via /status even while the goroutine is alive and retrying inside
// Run() — a goroutine in a retry loop is "running" by the supervisor's
// definition, but operators need to see the connectivity problem.
type ConnectivityMonitor interface {
	MonitorRunner
	// ConnectivityError returns the current connectivity failure, or nil when
	// the monitor's last operation completed successfully. Recovery (a
	// successful list + watch cycle) must clear the error within one cycle.
	//
	// Concurrency: implementations MUST make this safe for concurrent read.
	// The daemon's HTTP server goroutine calls ConnectivityError (via
	// statusSnapshot) while the monitor's own Run goroutine is writing the
	// underlying field. Guard the field with an atomic (e.g.
	// atomic.Pointer[StatusError]) or a mutex; a plain field read/written
	// from both goroutines is a data race.
	ConnectivityError() *StatusError
}

// StatusTracker holds the latest observed state for a set of monitors. It is
// safe for concurrent use; SupervisedMonitor updates it on each state transition.
type StatusTracker struct {
	mu     sync.RWMutex
	states map[string]MonitorState
}

// NewStatusTracker returns an empty StatusTracker.
func NewStatusTracker() *StatusTracker {
	return &StatusTracker{states: make(map[string]MonitorState)}
}

// set records a new state for the named monitor.
func (t *StatusTracker) set(name, state string) {
	t.mu.Lock()
	t.states[name] = MonitorState{State: state}
	t.mu.Unlock()
}

// Snapshot returns a copy of all monitor states at the time of the call.
func (t *StatusTracker) Snapshot() map[string]MonitorState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]MonitorState, len(t.states))
	for k, v := range t.states {
		out[k] = v
	}
	return out
}

// MonitorRunner is the interface that each long-running monitor goroutine must
// implement so it can be managed by SupervisedMonitor.
//
// Implementations must:
//   - Return nil when ctx is cancelled (clean shutdown, no restart).
//   - Return a non-nil error only on unexpected failure (triggers supervised restart).
//   - Be safe to call again after returning an error (the supervisor calls Run again).
//   - Not leak unguarded goroutines: the supervisor's panic recovery covers only
//     the Run goroutine, so any goroutine the monitor spawns must own its own
//     recover — use Go or Group (see Run). An unguarded panic in a spawned
//     goroutine crashes the daemon.
type MonitorRunner interface {
	// Run starts the monitor and blocks until ctx is cancelled or the monitor
	// encounters an unrecoverable error. A nil return means clean shutdown; a
	// non-nil return triggers a supervised restart with back-off.
	//
	// Panic scope: SupervisedMonitor invokes Run behind a recover barrier (see
	// safeRun), so a panic on the Run goroutine is converted into a crash and
	// restarted with back-off rather than taking down the process. That barrier
	// is per-goroutine and covers ONLY the goroutine executing Run. A monitor
	// that spawns its own goroutines (go func(){…}) owns their recovery: an
	// unguarded panic in a child goroutine unwinds independently, is NOT caught
	// by the supervisor, and still crashes the daemon. So a monitor must not
	// spawn a bare goroutine — use Group for a child whose failure should restart
	// the monitor (its panic/error surfaces from Wait as the Run error), or Go
	// for best-effort work that must not (its panic is recovered and logged). The
	// barrier also cannot catch fatal runtime errors (concurrent map writes,
	// stack overflow, OOM, all-goroutine deadlock, cgo SIGSEGV): those are
	// runtime.throw, not panics, and remain fatal by design.
	Run(ctx context.Context) error

	// Name returns a stable, human-readable identifier for the monitor used
	// in structured log entries (e.g. "upgrade-monitor", "migration-monitor").
	Name() string
}

// SupervisorOptions groups the optional dependencies and tunables for
// SupervisedMonitor. The zero value is valid: no status tracking, a silent
// (discard) logger, and no heartbeat.
type SupervisorOptions struct {
	// Tracker, when non-nil, is updated on every monitor state transition so the
	// /status endpoint can report per-monitor state without polling. May be nil.
	Tracker *StatusTracker

	// Logger is the structured logger the supervisor logs through (crash,
	// back-off, degradation, heartbeat, clean exit). When nil the supervisor
	// logs to a no-op discard logger and stays silent — it never writes to the
	// global slog default implicitly. Inject a logger
	// (e.g. slog.New(logx.NewSlogHandler())) to route diagnostics to your
	// logging backend.
	Logger *slog.Logger

	// HeartbeatInterval, when greater than zero, makes the supervisor emit a
	// periodic MonitorHeartbeat Info record (with the monitor name and its
	// current uptime) while the monitor is in the running state. This lets a
	// remote observability backend detect an alive-but-wedged monitor — one
	// blocked inside Run, never crashing and never logging — by the ABSENCE of
	// heartbeats. Zero (the default) disables heartbeats entirely.
	HeartbeatInterval time.Duration
}

// SupervisedMonitor runs m in a restart loop. When m.Run returns a non-nil
// error the supervisor waits for a back-off delay and then restarts it.
// Clean shutdown (nil return or ctx cancellation) exits the loop immediately
// without restarting.
//
// Back-off strategy:
//   - Starts at supervisedBackoffInitial (5 s).
//   - Doubles on each crash up to supervisedBackoffCap (5 min).
//   - Resets to supervisedBackoffInitial when the monitor runs stably for
//     at least supervisedStableThreshold (60 s) before the next crash.
//
// Degradation alerting:
//   - Tracks consecutive crashes (resets after a stable run).
//   - Emits a MonitorDegraded Error log every supervisedDegradedThreshold
//     consecutive crashes (at crash #5, #10, #15, …) so ops keeps seeing
//     the alert as long as the monitor remains degraded.
//
// Heartbeats:
//   - When opts.HeartbeatInterval > 0, a MonitorHeartbeat Info record is
//     emitted on that interval while the monitor is running, so remote
//     observability can alert on the absence of heartbeats.
//
// Panic recovery:
//   - m.Run is invoked through a recover barrier (see safeRun): a panic on the
//     Run goroutine is converted into a synthetic error and handled on the
//     normal crash path (MonitorCrash log carrying the monitor name, the
//     recovered value, and a stack trace, followed by back-off and restart) —
//     a monitor panic never crashes the daemon. This covers only the Run
//     goroutine; see MonitorRunner.Run for the goroutine-scope and
//     fatal-runtime-error boundaries.
//
// This function never returns an error — it absorbs crashes and restarts the
// monitor indefinitely until ctx is cancelled.
//
// See SupervisorOptions for the meaning of each field; the zero value is valid.
func SupervisedMonitor(ctx context.Context, m MonitorRunner, opts SupervisorOptions) {
	log := loggerOrDiscard(opts.Logger)
	tracker := opts.Tracker
	backoff := supervisedBackoffInitial
	consecutiveCrashes := 0

	setState := func(state string) {
		if tracker != nil {
			tracker.set(m.Name(), state)
		}
	}

	for {
		start := time.Now()
		setState("running")

		// Heartbeat goroutine for this run. Independent of the work loop so a
		// monitor that legitimately blocks for a long time (e.g. a long watch)
		// still heartbeats. Bounded by runCtx, cancelled the instant Run returns.
		runCtx, stopHeartbeat := context.WithCancel(ctx)
		if opts.HeartbeatInterval > 0 {
			// Heartbeat runs via Go so a panic in a monitor's ConnectivityError
			// (called below) is recovered and logged rather than crashing the
			// daemon — a diagnostic beacon must never take down real work.
			Go(runCtx, log, m.Name()+" heartbeat", func(ctx context.Context) {
				t := time.NewTicker(opts.HeartbeatInterval)
				defer t.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-t.C:
						attrs := []any{
							"reason", "MonitorHeartbeat",
							"monitor", m.Name(),
							"uptime", time.Since(start),
						}
						// Carry liveness AND health: a monitor that is
						// "running" but failing (e.g. stuck in a watch-retry
						// loop) reports its connectivity error here, so a
						// missing heartbeat means dead/wedged while
						// healthy=false means alive-but-failing — both
						// alertable from the push stream without a /status
						// scrape. Monitors that can't observe their own health
						// omit the field entirely.
						if cm, ok := m.(ConnectivityMonitor); ok {
							if connErr := cm.ConnectivityError(); connErr != nil {
								attrs = append(attrs,
									"healthy", false,
									"connectivity_reason", connErr.Reason,
									"connectivity_error", connErr.Message)
							} else {
								attrs = append(attrs, "healthy", true)
							}
						}
						log.Info("Monitor heartbeat", attrs...)
					}
				}
			})
		}

		// safeRun wraps m.Run in a recover barrier so a panic on the Run
		// goroutine becomes a crash handled below (back-off + restart) rather
		// than an un-recovered panic that takes down the whole daemon.
		err := safeRun(ctx, m)
		stopHeartbeat()

		// ctx cancelled → clean shutdown, do not restart.
		if ctx.Err() != nil {
			setState("stopped")
			log.Info("Monitor stopped cleanly",
				"reason", "MonitorStopped",
				"monitor", m.Name())
			return
		}

		// nil return without ctx cancellation → also clean exit.
		if err == nil {
			setState("stopped")
			log.Info("Monitor exited without error and without context cancellation — not restarting",
				"reason", "MonitorExited",
				"monitor", m.Name())
			return
		}

		// Crash path.
		//
		// A stable run before this crash means the previous failure streak
		// recovered: reset both the back-off and the consecutive-crash counter
		// BEFORE counting this crash, so the crash that ended a stable period is
		// counted as #1 of a fresh streak. Doing this after the increment would
		// (a) let a post-stable crash spuriously trip the degraded threshold when
		// its pre-reset count happened to land on a multiple of it, and (b) drop
		// that crash from the new streak's count entirely.
		if time.Since(start) >= supervisedStableThreshold {
			backoff = supervisedBackoffInitial
			consecutiveCrashes = 0
		}

		consecutiveCrashes++

		log.Error("Monitor crashed — restarting after back-off",
			"error", err,
			"reason", "MonitorCrash",
			"monitor", m.Name(),
			"consecutive_crashes", consecutiveCrashes,
			"backoff", backoff)

		// Emit MonitorDegraded at every supervisedDegradedThreshold consecutive
		// crashes so ops is alerted at crash #5, #10, #15, …
		if consecutiveCrashes%supervisedDegradedThreshold == 0 {
			log.Error("Monitor is crashing repeatedly — operator intervention may be required",
				"error", err,
				"reason", "MonitorDegraded",
				"monitor", m.Name(),
				"consecutive_crashes", consecutiveCrashes,
				"current_backoff", backoff)
		}

		setState(fmt.Sprintf("backoff:%s", backoff))

		select {
		case <-ctx.Done():
			setState("stopped")
			log.Info("Monitor restart cancelled — context done",
				"reason", "MonitorStopped",
				"monitor", m.Name())
			return
		case <-time.After(backoff):
		}

		// Grow backoff for the next potential crash (capped).
		// Note: the updated value takes effect on the *next* crash, not the current
		// one. So the wait sequence for consecutive crashes is:
		//   crash 1 → sleep 5s  (initial)
		//   crash 2 → sleep 10s
		//   crash 3 → sleep 20s … up to 5 min cap
		// This is intentional: the first sleep gives the system a moment to recover;
		// subsequent sleeps grow to reduce pressure during sustained failures.
		nextBackoff := time.Duration(float64(backoff) * supervisedBackoffMultiplier)
		if nextBackoff > supervisedBackoffCap {
			nextBackoff = supervisedBackoffCap
		}
		backoff = nextBackoff
	}
}

// safeRun invokes m.Run(ctx) behind recoverToErr so a panic on the Run goroutine
// becomes a supervised restart instead of a process crash: the recovered panic
// is returned as an error and flows through SupervisedMonitor's normal crash
// path (back-off, MonitorCrash / MonitorDegraded, restart) identically to an
// error returned by Run. A nil or error return from Run passes through unchanged.
//
// See recoverToErr for the goroutine-scope and fatal-runtime-error boundaries,
// and MonitorRunner.Run for the contract that monitor-spawned goroutines carry
// their own recovery (via Go or Group).
func safeRun(ctx context.Context, m MonitorRunner) error {
	return recoverToErr(fmt.Sprintf("monitor %q", m.Name()), func() error {
		return m.Run(ctx)
	})
}
