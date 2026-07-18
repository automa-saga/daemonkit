// SPDX-License-Identifier: Apache-2.0

package daemonkit

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/joomcode/errorx"
	"golang.org/x/sync/errgroup"
)

// recoverToErr runs fn and converts a panic into a non-nil error, so a panicking
// unit of work can be handled like any other failure instead of unwinding to the
// top of its goroutine and crashing the process. The returned error is an
// errorx.InternalError (a panic is always a bug) whose message carries what
// panicked, the recovered value, and the stack trace captured at recover time
// (the stack is unwound once the deferred func returns, so it must be captured
// here). A nil or error return from fn passes through unchanged.
//
// recoverToErr is the shared core behind safeRun, Group.Go, and Go. `what` is a
// human label for the unit of work (e.g. `monitor "marker-monitor"` or
// `goroutine "watch"`) used verbatim in the error message.
//
// Boundaries: recover is per-goroutine, so this only guards a panic raised on
// the goroutine that calls fn — a panic in a further goroutine fn spawns is not
// caught. It also cannot catch fatal runtime errors (concurrent map writes,
// stack overflow, OOM, all-goroutine deadlock, cgo SIGSEGV): those are
// runtime.throw, not panics, and terminate the process by design.
func recoverToErr(what string, fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errorx.InternalError.New("%s panicked: %v\n%s", what, r, debug.Stack())
		}
	}()
	return fn()
}

// Go runs fn in a new goroutine behind a panic barrier: if fn panics, the panic
// is recovered and logged (reason "GoroutinePanic", with the name, recovered
// value, and stack) instead of crashing the daemon, and the goroutine stops.
//
// Go is for best-effort, fire-and-forget background work a monitor spawns whose
// failure should NOT tear down the monitor — e.g. emitting a heartbeat or a
// metric. Because the panic is swallowed, the monitor keeps running as if the
// goroutine were still alive; do NOT use Go for work the monitor depends on. For
// a goroutine whose panic (or error) should restart the monitor, use Group
// instead, so the failure propagates out of Run to the supervisor.
//
// log receives the recovery record; a nil log falls back to the kit's discard
// logger, which makes the panic silent — pass a real logger unless a swallowed,
// unlogged panic is genuinely acceptable. fn should return when ctx is done.
func Go(ctx context.Context, log *slog.Logger, name string, fn func(context.Context)) {
	l := loggerOrDiscard(log)
	go func() {
		if err := recoverToErr(fmt.Sprintf("goroutine %q", name), func() error {
			fn(ctx)
			return nil
		}); err != nil {
			l.Error("Goroutine panicked — recovered",
				"error", err,
				"reason", "GoroutinePanic",
				"goroutine", name)
		}
	}()
}

// Group is a panic-recovering wrapper around golang.org/x/sync/errgroup.Group
// for the goroutines a monitor spawns whose failure should restart the monitor.
// Each child runs behind recoverToErr, so a panicking child becomes a non-nil
// error (rather than a process crash); Wait returns the first non-nil error
// (panic or returned) and, as with errgroup, the first failure cancels the group
// context so siblings can exit.
//
// The intended shape is for a monitor's Run to fan out its long-running loops
// through a Group and return Wait, so any child failure surfaces as the Run error
// and the supervisor restarts the whole monitor with a clean slate:
//
//	func (m *M) Run(ctx context.Context) error {
//		g, ctx := daemonkit.WithContext(ctx)
//		g.Go("watch", m.watchLoop)
//		g.Go("prune", m.pruneLoop)
//		return g.Wait() // a child panic/error → Run error → supervised restart
//	}
//
// Use Group when a child is essential to the monitor; use Go for best-effort work
// that must not restart the monitor.
type Group struct {
	eg  *errgroup.Group
	ctx context.Context
}

// WithContext returns a Group and a derived context that is cancelled the moment
// the first child returns a non-nil error or panics (mirroring
// errgroup.WithContext). Children receive this context via Group.Go; the returned
// context is also handed back so the caller can pass it to work outside the group.
func WithContext(ctx context.Context) (*Group, context.Context) {
	eg, gctx := errgroup.WithContext(ctx)
	return &Group{eg: eg, ctx: gctx}, gctx
}

// Go runs fn in a new goroutine under the group, behind a panic barrier. fn
// receives the group's derived context (cancelled when any child fails, so fn
// must return promptly once it is done). A panic in fn is converted to an error
// carrying name, the recovered value, and a stack, and is returned by Wait like
// any other child error. name identifies the child in that error.
func (g *Group) Go(name string, fn func(context.Context) error) {
	g.eg.Go(func() error {
		return recoverToErr(fmt.Sprintf("goroutine %q", name), func() error {
			return fn(g.ctx)
		})
	})
}

// Wait blocks until all children spawned via Go have returned, then returns the
// first non-nil error (a returned error or a recovered panic), or nil if every
// child succeeded.
func (g *Group) Wait() error {
	return g.eg.Wait()
}
