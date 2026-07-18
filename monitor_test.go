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

	"github.com/stretchr/testify/assert"
)

// ---- test helpers ----

type fakeMonitor struct {
	name     string
	runs     atomic.Int32 // incremented at the start of each Run call
	behavior func(ctx context.Context, run int) error
}

func (f *fakeMonitor) Name() string { return f.name }
func (f *fakeMonitor) Run(ctx context.Context) error {
	run := int(f.runs.Add(1))
	return f.behavior(ctx, run)
}

// reasonCapture is a slog.Handler that records the "reason" attribute of every
// record so tests can assert which structured events were (or were not) emitted.
type reasonCapture struct {
	mu      sync.Mutex
	reasons []string
}

func (c *reasonCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *reasonCapture) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *reasonCapture) WithGroup(string) slog.Handler            { return c }
func (c *reasonCapture) Handle(_ context.Context, r slog.Record) error {
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "reason" {
			c.mu.Lock()
			c.reasons = append(c.reasons, a.Value.String())
			c.mu.Unlock()
		}
		return true
	})
	return nil
}

func (c *reasonCapture) count(reason string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, r := range c.reasons {
		if r == reason {
			n++
		}
	}
	return n
}

// captureSlogReasons installs a reasonCapture as the default slog logger for the
// duration of the test and returns it.
func captureSlogReasons(t *testing.T) *reasonCapture {
	t.Helper()
	cap := &reasonCapture{}
	orig := slog.Default()
	slog.SetDefault(slog.New(cap))
	t.Cleanup(func() { slog.SetDefault(orig) })
	return cap
}

// ---- tests ----

func TestSupervisedMonitor_RestartsAfterCrash(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const wantRuns = 3
	m := &fakeMonitor{
		name: "test-monitor",
		behavior: func(ctx context.Context, run int) error {
			if run < wantRuns {
				return errors.New("simulated crash")
			}
			<-ctx.Done()
			return nil
		},
	}

	origInitial := supervisedBackoffInitial
	supervisedBackoffInitial = 1 * time.Millisecond
	t.Cleanup(func() { supervisedBackoffInitial = origInitial })

	done := make(chan struct{})
	go func() {
		SupervisedMonitor(ctx, m, SupervisorOptions{Logger: slog.Default()})
		close(done)
	}()

	assert.Eventually(t, func() bool {
		return int(m.runs.Load()) >= wantRuns
	}, 5*time.Second, 5*time.Millisecond, "expected at least %d Run calls", wantRuns)

	cancel()
	<-done
	assert.GreaterOrEqual(t, int(m.runs.Load()), wantRuns)
}

// crashCapture records the "error" attribute of the first MonitorCrash record so
// panic-recovery tests can assert the crash log carried the monitor name, the
// recovered value, and a stack trace.
type crashCapture struct {
	mu       sync.Mutex
	crashErr string
}

func (c *crashCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *crashCapture) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *crashCapture) WithGroup(string) slog.Handler            { return c }
func (c *crashCapture) Handle(_ context.Context, r slog.Record) error {
	var reason, errStr string
	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "reason":
			reason = a.Value.String()
		case "error":
			errStr = a.Value.String()
		}
		return true
	})
	if reason == "MonitorCrash" {
		c.mu.Lock()
		if c.crashErr == "" {
			c.crashErr = errStr
		}
		c.mu.Unlock()
	}
	return nil
}

func (c *crashCapture) crashError() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.crashErr
}

// TestSafeRun_RecoversPanicIntoError verifies the panic barrier in isolation: a
// panicking Run becomes a non-nil error carrying the monitor name, the recovered
// value, and a stack trace, while a nil return and a returned error both pass
// through unchanged (a recovered panic must be indistinguishable from a returned
// error to the crash path).
func TestSafeRun_RecoversPanicIntoError(t *testing.T) {
	t.Run("panic becomes an error with name, value, and stack", func(t *testing.T) {
		m := &fakeMonitor{
			name:     "boom-monitor",
			behavior: func(context.Context, int) error { panic("kaboom") },
		}
		err := safeRun(context.Background(), m)
		if assert.Error(t, err, "a panicking Run must yield a non-nil error") {
			assert.Contains(t, err.Error(), "boom-monitor", "error must name the monitor")
			assert.Contains(t, err.Error(), "kaboom", "error must carry the recovered value")
			assert.Contains(t, err.Error(), "goroutine", "error must embed a stack trace")
		}
	})

	t.Run("nil return passes through", func(t *testing.T) {
		m := &fakeMonitor{
			name:     "clean-monitor",
			behavior: func(context.Context, int) error { return nil },
		}
		assert.NoError(t, safeRun(context.Background(), m))
	})

	t.Run("returned error passes through unchanged", func(t *testing.T) {
		sentinel := errors.New("plain failure")
		m := &fakeMonitor{
			name:     "erroring-monitor",
			behavior: func(context.Context, int) error { return sentinel },
		}
		assert.ErrorIs(t, safeRun(context.Background(), m), sentinel)
	})
}

// TestSupervisedMonitor_RestartsAfterPanic asserts a monitor that panics on its
// first Run is recovered, restarted through the normal back-off path, and reaches
// a stable running state — proving a monitor panic never crashes the daemon. It
// also asserts the MonitorCrash log carried the monitor name, the recovered
// value, and a stack trace.
func TestSupervisedMonitor_RestartsAfterPanic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	origInitial := supervisedBackoffInitial
	supervisedBackoffInitial = 1 * time.Millisecond
	t.Cleanup(func() { supervisedBackoffInitial = origInitial })

	const wantRuns = 2
	m := &fakeMonitor{
		name: "panicky-monitor",
		behavior: func(ctx context.Context, run int) error {
			if run < wantRuns {
				panic("boom on first run")
			}
			<-ctx.Done() // run 2 blocks (stable) until the test cancels
			return nil
		},
	}

	cap := &crashCapture{}
	done := make(chan struct{})
	go func() {
		SupervisedMonitor(ctx, m, SupervisorOptions{Logger: slog.New(cap)})
		close(done)
	}()

	assert.Eventually(t, func() bool {
		return int(m.runs.Load()) >= wantRuns
	}, 5*time.Second, 1*time.Millisecond, "a panicking monitor must be restarted to a stable run")

	cancel()
	<-done

	crashErr := cap.crashError()
	assert.Contains(t, crashErr, "panicky-monitor", "crash log must name the monitor")
	assert.Contains(t, crashErr, "boom on first run", "crash log must carry the recovered panic value")
	assert.Contains(t, crashErr, "goroutine", "crash log must carry a stack trace")
}

func TestSupervisedMonitor_NoRestartOnCtxCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	m := &fakeMonitor{
		name: "test-monitor",
		behavior: func(ctx context.Context, run int) error {
			cancel()
			return errors.New("error concurrent with cancel")
		},
	}

	done := make(chan struct{})
	go func() {
		SupervisedMonitor(ctx, m, SupervisorOptions{Logger: slog.Default()})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("SupervisedMonitor did not exit after context cancellation")
	}

	assert.Equal(t, int32(1), m.runs.Load())
}

func TestSupervisedMonitor_BackoffResetAfterStableRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	origInitial := supervisedBackoffInitial
	origStable := supervisedStableThreshold
	supervisedBackoffInitial = 1 * time.Millisecond
	supervisedStableThreshold = 5 * time.Millisecond
	t.Cleanup(func() {
		supervisedBackoffInitial = origInitial
		supervisedStableThreshold = origStable
	})

	const wantRuns = 4
	m := &fakeMonitor{
		name: "test-monitor",
		behavior: func(ctx context.Context, run int) error {
			if run < wantRuns {
				time.Sleep(2 * supervisedStableThreshold)
				return errors.New("simulated crash after stable run")
			}
			<-ctx.Done()
			return nil
		},
	}

	done := make(chan struct{})
	go func() {
		SupervisedMonitor(ctx, m, SupervisorOptions{Logger: slog.Default()})
		close(done)
	}()

	assert.Eventually(t, func() bool {
		return int(m.runs.Load()) >= wantRuns
	}, 5*time.Second, 5*time.Millisecond, "expected at least %d Run calls", wantRuns)

	cancel()
	<-done
	assert.GreaterOrEqual(t, int(m.runs.Load()), wantRuns)
}

func TestSupervisedMonitor_DegradedEventFired(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	origInitial := supervisedBackoffInitial
	origThreshold := supervisedDegradedThreshold
	supervisedBackoffInitial = 1 * time.Millisecond
	supervisedDegradedThreshold = 3
	t.Cleanup(func() {
		supervisedBackoffInitial = origInitial
		supervisedDegradedThreshold = origThreshold
	})

	const wantRuns = 7
	m := &fakeMonitor{
		name: "test-monitor",
		behavior: func(ctx context.Context, run int) error {
			if run < wantRuns {
				return errors.New("crash")
			}
			<-ctx.Done()
			return nil
		},
	}

	done := make(chan struct{})
	go func() {
		SupervisedMonitor(ctx, m, SupervisorOptions{Logger: slog.Default()})
		close(done)
	}()

	assert.Eventually(t, func() bool {
		return int(m.runs.Load()) >= wantRuns
	}, 5*time.Second, 1*time.Millisecond, "expected at least %d Run calls", wantRuns)

	cancel()
	<-done
	assert.GreaterOrEqual(t, int(m.runs.Load()), wantRuns)
}

func TestSupervisedMonitor_DegradedCounterResetsAfterStableRun(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	origInitial := supervisedBackoffInitial
	origStable := supervisedStableThreshold
	origThreshold := supervisedDegradedThreshold
	supervisedBackoffInitial = 1 * time.Millisecond
	supervisedStableThreshold = 5 * time.Millisecond
	supervisedDegradedThreshold = 3
	t.Cleanup(func() {
		supervisedBackoffInitial = origInitial
		supervisedStableThreshold = origStable
		supervisedDegradedThreshold = origThreshold
	})

	cap := captureSlogReasons(t)

	// threshold=3. Crashes 1,2 are fast. Run 3 is stable (>= threshold) then
	// crashes: the stable run must reset the streak so this crash is #1 of a new
	// streak — NOT crash #3 — and must therefore NOT fire MonitorDegraded. Run 4
	// is a fast crash (streak #2). At no point does the streak reach 3, so
	// MonitorDegraded must never fire.
	const wantRuns = 5
	m := &fakeMonitor{
		name: "test-monitor",
		behavior: func(ctx context.Context, run int) error {
			switch run {
			case 1, 2:
				return errors.New("fast crash")
			case 3:
				time.Sleep(2 * supervisedStableThreshold)
				return errors.New("crash after stable run")
			case 4:
				return errors.New("fast crash after reset")
			default:
				<-ctx.Done()
				return nil
			}
		},
	}

	done := make(chan struct{})
	go func() {
		SupervisedMonitor(ctx, m, SupervisorOptions{Logger: slog.Default()})
		close(done)
	}()

	assert.Eventually(t, func() bool {
		return int(m.runs.Load()) >= wantRuns
	}, 5*time.Second, 1*time.Millisecond, "expected at least %d Run calls", wantRuns)

	cancel()
	<-done

	assert.Equal(t, 0, cap.count("MonitorDegraded"),
		"a crash ending a stable run must start a fresh streak and not trip the degraded threshold")
}

func TestSupervisedMonitor_BackoffCapAtMax(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	origInitial := supervisedBackoffInitial
	origCap := supervisedBackoffCap
	supervisedBackoffInitial = 1 * time.Millisecond
	supervisedBackoffCap = 8 * time.Millisecond
	t.Cleanup(func() {
		supervisedBackoffInitial = origInitial
		supervisedBackoffCap = origCap
	})

	const wantRuns = 6
	m := &fakeMonitor{
		name: "test-monitor",
		behavior: func(ctx context.Context, run int) error {
			if run < wantRuns {
				return errors.New("crash")
			}
			<-ctx.Done()
			return nil
		},
	}

	done := make(chan struct{})
	go func() {
		SupervisedMonitor(ctx, m, SupervisorOptions{Logger: slog.Default()})
		close(done)
	}()

	assert.Eventually(t, func() bool {
		return int(m.runs.Load()) >= wantRuns
	}, 1*time.Second, 1*time.Millisecond, "expected at least %d Run calls", wantRuns)

	cancel()
	<-done
}

func TestSupervisedMonitor_UsesInjectedLoggerNotDefault(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	origInitial := supervisedBackoffInitial
	origThreshold := supervisedDegradedThreshold
	supervisedBackoffInitial = 1 * time.Millisecond
	supervisedDegradedThreshold = 1 // emit MonitorDegraded on every crash
	t.Cleanup(func() {
		supervisedBackoffInitial = origInitial
		supervisedDegradedThreshold = origThreshold
	})

	// The default logger must NOT receive the kit's records; the injected one must.
	defaultCap := captureSlogReasons(t)
	injected := &reasonCapture{}

	const wantRuns = 3
	m := &fakeMonitor{
		name: "test-monitor",
		behavior: func(ctx context.Context, run int) error {
			if run < wantRuns {
				return errors.New("crash")
			}
			<-ctx.Done()
			return nil
		},
	}

	done := make(chan struct{})
	go func() {
		SupervisedMonitor(ctx, m, SupervisorOptions{Logger: slog.New(injected)})
		close(done)
	}()

	assert.Eventually(t, func() bool {
		return injected.count("MonitorCrash") >= 1
	}, 3*time.Second, 1*time.Millisecond, "injected logger should receive crash records")

	cancel()
	<-done

	assert.GreaterOrEqual(t, injected.count("MonitorDegraded"), 1,
		"injected logger should receive degradation records")
	assert.Equal(t, 0, defaultCap.count("MonitorCrash"),
		"the global slog default must not receive the kit's records when a logger is injected")
}

func TestSupervisedMonitor_NilLoggerIsSilentAndSafe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	m := &fakeMonitor{
		name: "test-monitor",
		behavior: func(ctx context.Context, run int) error {
			cancel()
			return nil
		},
	}

	done := make(chan struct{})
	go func() {
		// A nil logger must not panic; the kit falls back to a discard logger.
		SupervisedMonitor(ctx, m, SupervisorOptions{})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("SupervisedMonitor with a nil logger did not exit")
	}
}

// healthCapture records the "reason" and "healthy" attrs of every record so
// heartbeat tests can assert both that heartbeats fired and what health they
// carried.
type healthCapture struct {
	mu        sync.Mutex
	reasons   []string
	healthy   []bool // one entry per record that carried a "healthy" attr
	connError []string
}

func (c *healthCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *healthCapture) WithAttrs([]slog.Attr) slog.Handler       { return c }
func (c *healthCapture) WithGroup(string) slog.Handler            { return c }
func (c *healthCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "reason":
			c.reasons = append(c.reasons, a.Value.String())
		case "healthy":
			c.healthy = append(c.healthy, a.Value.Bool())
		case "connectivity_reason":
			c.connError = append(c.connError, a.Value.String())
		}
		return true
	})
	return nil
}

func (c *healthCapture) count(reason string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, r := range c.reasons {
		if r == reason {
			n++
		}
	}
	return n
}

func (c *healthCapture) sawUnhealthy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, h := range c.healthy {
		if !h {
			return true
		}
	}
	return false
}

// fakeConnMonitor is a MonitorRunner that also reports a connectivity error,
// guarded for concurrent read as the ConnectivityMonitor contract requires.
type fakeConnMonitor struct {
	fakeMonitor
	mu      sync.Mutex
	connErr *StatusError
}

func (m *fakeConnMonitor) ConnectivityError() *StatusError {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connErr
}

func (m *fakeConnMonitor) setConnErr(e *StatusError) {
	m.mu.Lock()
	m.connErr = e
	m.mu.Unlock()
}

func TestSupervisedMonitor_HeartbeatDisabledByDefault(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cap := &healthCapture{}
	m := &fakeMonitor{
		name: "test-monitor",
		behavior: func(ctx context.Context, _ int) error {
			<-ctx.Done()
			return nil
		},
	}

	done := make(chan struct{})
	go func() {
		// No HeartbeatInterval set → heartbeats must never fire.
		SupervisedMonitor(ctx, m, SupervisorOptions{Logger: slog.New(cap)})
		close(done)
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done

	assert.Equal(t, 0, cap.count("MonitorHeartbeat"),
		"heartbeats must not fire when HeartbeatInterval is zero")
}

func TestSupervisedMonitor_HeartbeatFiresAndCarriesHealth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cap := &healthCapture{}
	m := &fakeConnMonitor{
		fakeMonitor: fakeMonitor{
			name: "conn-monitor",
			behavior: func(ctx context.Context, _ int) error {
				<-ctx.Done()
				return nil
			},
		},
	}

	done := make(chan struct{})
	go func() {
		SupervisedMonitor(ctx, m, SupervisorOptions{
			Logger:            slog.New(cap),
			HeartbeatInterval: 10 * time.Millisecond,
		})
		close(done)
	}()

	// Healthy heartbeats first.
	assert.Eventually(t, func() bool {
		return cap.count("MonitorHeartbeat") >= 2
	}, 2*time.Second, 5*time.Millisecond, "expected periodic heartbeats")

	// Now the monitor goes unhealthy while still running; heartbeats must
	// reflect it.
	m.setConnErr(&StatusError{Reason: "WatchFailed", Message: "connection refused"})
	assert.Eventually(t, func() bool {
		return cap.sawUnhealthy()
	}, 2*time.Second, 5*time.Millisecond, "heartbeat should carry healthy=false once the monitor is failing")

	cancel()
	<-done
}

// panicConnMonitor is a ConnectivityMonitor whose ConnectivityError panics. It
// proves the heartbeat goroutine — which calls ConnectivityError — recovers via
// Go rather than crashing the daemon.
type panicConnMonitor struct {
	fakeMonitor
}

func (m *panicConnMonitor) ConnectivityError() *StatusError {
	panic("boom in ConnectivityError")
}

// TestSupervisedMonitor_HeartbeatPanicRecovered asserts a panic raised inside the
// heartbeat goroutine (here from a monitor's ConnectivityError) is recovered and
// logged with reason GoroutinePanic instead of crashing the daemon — a regression
// guard for the heartbeat being spawned via Go.
func TestSupervisedMonitor_HeartbeatPanicRecovered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cap := &reasonCapture{}
	m := &panicConnMonitor{
		fakeMonitor: fakeMonitor{
			name: "panic-conn-monitor",
			behavior: func(ctx context.Context, _ int) error {
				<-ctx.Done()
				return nil
			},
		},
	}

	done := make(chan struct{})
	go func() {
		SupervisedMonitor(ctx, m, SupervisorOptions{
			Logger:            slog.New(cap),
			HeartbeatInterval: 5 * time.Millisecond,
		})
		close(done)
	}()

	assert.Eventually(t, func() bool {
		return cap.count("GoroutinePanic") >= 1
	}, 2*time.Second, 5*time.Millisecond,
		"a panic in the heartbeat must be recovered and logged, not crash the daemon")

	cancel()
	<-done // if the daemon had crashed on the heartbeat panic, we'd never reach here
}

// TestSupervisedMonitor_PanicsTripDegradation asserts consecutive panics are
// accounted identically to consecutive returned errors: they accumulate and fire
// MonitorDegraded (acceptance criterion: a recovered panic is just another crash).
func TestSupervisedMonitor_PanicsTripDegradation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	origInitial := supervisedBackoffInitial
	origThreshold := supervisedDegradedThreshold
	supervisedBackoffInitial = 1 * time.Millisecond
	supervisedDegradedThreshold = 3
	t.Cleanup(func() {
		supervisedBackoffInitial = origInitial
		supervisedDegradedThreshold = origThreshold
	})

	cap := &reasonCapture{}
	const wantRuns = 4
	m := &fakeMonitor{
		name: "panicky-monitor",
		behavior: func(ctx context.Context, run int) error {
			if run < wantRuns {
				panic("boom")
			}
			<-ctx.Done()
			return nil
		},
	}

	done := make(chan struct{})
	go func() {
		SupervisedMonitor(ctx, m, SupervisorOptions{Logger: slog.New(cap)})
		close(done)
	}()

	assert.Eventually(t, func() bool {
		return cap.count("MonitorDegraded") >= 1
	}, 5*time.Second, 1*time.Millisecond,
		"consecutive panics must accumulate and fire MonitorDegraded like returned errors")

	cancel()
	<-done
}
