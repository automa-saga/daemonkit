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
