// SPDX-License-Identifier: Apache-2.0

//go:build !integration

package daemonkit

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/joomcode/errorx"
	"github.com/stretchr/testify/assert"
)

// ---- recoverToErr (shared core) ----

// TestRecoverToErr covers the shared recover core directly: a panic becomes an
// errorx.InternalError carrying the label, the recovered value, and a stack;
// non-panic returns pass through unchanged; and it catches non-string and nil
// panic values too (a bug can panic with anything).
func TestRecoverToErr(t *testing.T) {
	t.Run("panic becomes an InternalError with label, value, and stack", func(t *testing.T) {
		err := recoverToErr(`monitor "x"`, func() error { panic("boom") })
		if assert.Error(t, err) {
			assert.True(t, errorx.IsOfType(err, errorx.InternalError),
				"a recovered panic must be typed errorx.InternalError")
			assert.Contains(t, err.Error(), `monitor "x"`, "must carry the label")
			assert.Contains(t, err.Error(), "boom", "must carry the recovered value")
			assert.Contains(t, err.Error(), "goroutine", "must embed a stack trace")
		}
	})

	t.Run("nil return passes through", func(t *testing.T) {
		assert.NoError(t, recoverToErr("x", func() error { return nil }))
	})

	t.Run("returned error passes through unchanged", func(t *testing.T) {
		sentinel := errors.New("plain failure")
		assert.ErrorIs(t, recoverToErr("x", func() error { return sentinel }), sentinel)
	})

	t.Run("recovers a non-string panic value", func(t *testing.T) {
		err := recoverToErr("x", func() error { panic(errors.New("panic-with-error")) })
		if assert.Error(t, err) {
			assert.Contains(t, err.Error(), "panic-with-error")
		}
	})

	t.Run("recovers panic(nil)", func(t *testing.T) {
		err := recoverToErr("x", func() error { panic(nil) })
		assert.Error(t, err,
			"panic(nil) yields *runtime.PanicNilError in Go 1.21+ and must still be recovered")
	})
}

// ---- Group ----

// TestGroup_RecoversChildPanic asserts a panicking child does not crash the
// process: the panic is converted into the Wait error (carrying the child name,
// the recovered value, and a stack), and the first failure cancels the group
// context so siblings are torn down.
func TestGroup_RecoversChildPanic(t *testing.T) {
	g, gctx := WithContext(context.Background())

	siblingCancelled := make(chan struct{})
	g.Go("worker", func(ctx context.Context) error {
		panic("boom in worker")
	})
	g.Go("sibling", func(ctx context.Context) error {
		<-ctx.Done() // must be cancelled once the worker panics
		close(siblingCancelled)
		return ctx.Err()
	})

	err := g.Wait()
	if assert.Error(t, err, "a panicking child must surface as the Wait error") {
		assert.Contains(t, err.Error(), "worker", "error must name the panicking child")
		assert.Contains(t, err.Error(), "boom in worker", "error must carry the recovered value")
		assert.Contains(t, err.Error(), "goroutine", "error must embed a stack trace")
	}
	assert.Error(t, gctx.Err(), "the group context must be cancelled after a child fails")
	select {
	case <-siblingCancelled:
	default:
		t.Error("sibling was not cancelled when its sibling panicked")
	}
}

// TestGroup_PropagatesChildError asserts a normally-returned error propagates
// through Wait unchanged — a recovered panic and a returned error are handled
// identically.
func TestGroup_PropagatesChildError(t *testing.T) {
	sentinel := errors.New("child failed")
	g, _ := WithContext(context.Background())
	g.Go("worker", func(ctx context.Context) error { return sentinel })
	assert.ErrorIs(t, g.Wait(), sentinel)
}

// TestGroup_AllSucceed asserts Wait returns nil and every child ran when none
// fails.
func TestGroup_AllSucceed(t *testing.T) {
	g, _ := WithContext(context.Background())
	var ran atomic.Int32
	for i := 0; i < 3; i++ {
		g.Go("worker", func(ctx context.Context) error {
			ran.Add(1)
			return nil
		})
	}
	assert.NoError(t, g.Wait())
	assert.Equal(t, int32(3), ran.Load())
}

// ---- Go ----

// TestGo_RunsFunction asserts Go actually executes fn (and that a nil logger is
// safe on the success path).
func TestGo_RunsFunction(t *testing.T) {
	ran := make(chan struct{})
	Go(context.Background(), nil, "worker", func(ctx context.Context) {
		close(ran)
	})
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("Go did not run the function")
	}
}

// TestGo_RecoversPanicAndLogs asserts a panicking fire-and-forget goroutine is
// recovered (no process crash) and logged with reason GoroutinePanic — Go
// swallows the panic by design rather than propagating it.
func TestGo_RecoversPanicAndLogs(t *testing.T) {
	cap := &reasonCapture{}
	Go(context.Background(), slog.New(cap), "reaper", func(ctx context.Context) {
		panic("boom in reaper")
	})
	assert.Eventually(t, func() bool {
		return cap.count("GoroutinePanic") >= 1
	}, 2*time.Second, 5*time.Millisecond, "a panic must be recovered and logged")
}

// TestGo_NilLoggerSwallowsPanicSafely asserts a panic with a nil logger is
// recovered silently rather than crashing the daemon.
func TestGo_NilLoggerSwallowsPanicSafely(t *testing.T) {
	reached := make(chan struct{})
	Go(context.Background(), nil, "worker", func(ctx context.Context) {
		defer close(reached)
		panic("boom")
	})
	select {
	case <-reached:
	case <-time.After(2 * time.Second):
		t.Fatal("Go did not run the function")
	}
	// Reaching here without a crash proves the panic was recovered under a nil
	// (discard) logger; give the deferred recover a moment to complete.
	time.Sleep(20 * time.Millisecond)
}

// goroutinePanicCapture records the "goroutine" name of every GoroutinePanic
// record, so nested-Go tests can assert which goroutine's barrier caught a panic.
type goroutinePanicCapture struct {
	mu    sync.Mutex
	names []string
}

func (c *goroutinePanicCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *goroutinePanicCapture) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *goroutinePanicCapture) WithGroup(string) slog.Handler            { return c }
func (c *goroutinePanicCapture) Handle(_ context.Context, r slog.Record) error {
	var reason, name string
	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "reason":
			reason = a.Value.String()
		case "goroutine":
			name = a.Value.String()
		}
		return true
	})
	if reason == "GoroutinePanic" {
		c.mu.Lock()
		c.names = append(c.names, name)
		c.mu.Unlock()
	}
	return nil
}

func (c *goroutinePanicCapture) recovered() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.names...)
}

// TestGo_NestedGoRecoversDeepPanic proves the panic barrier is transitive: a
// goroutine spawned via Go that itself spawns further goroutines via Go is
// guarded at every level. A panic three levels deep is caught by that deepest
// goroutine's own barrier — logged once, named for the deepest goroutine — and
// never crashes the daemon nor bubbles up to the outer levels. This is what
// "always use Go instead of a bare go func()" buys, at any nesting depth.
func TestGo_NestedGoRecoversDeepPanic(t *testing.T) {
	cap := &goroutinePanicCapture{}
	log := slog.New(cap)

	Go(context.Background(), log, "level-1", func(ctx context.Context) {
		Go(ctx, log, "level-2", func(ctx context.Context) {
			Go(ctx, log, "level-3", func(ctx context.Context) {
				panic("boom three levels deep")
			})
		})
	})

	assert.Eventually(t, func() bool {
		return len(cap.recovered()) >= 1
	}, 2*time.Second, 5*time.Millisecond,
		"a panic in a nested Go must be recovered, not crash the daemon")

	// Exactly the deepest goroutine's barrier caught it: the panic neither
	// bubbled up to level-1/level-2 (those returned normally) nor fired twice.
	assert.Equal(t, []string{"level-3"}, cap.recovered(),
		"only the deepest goroutine's own barrier should have recovered the panic")
}
