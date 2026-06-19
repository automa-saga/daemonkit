// SPDX-License-Identifier: Apache-2.0

package daemonkit

import (
	"context"
	"log/slog"
	"net"
	"os"
	"strconv"
	"time"
)

const (
	sdReady    = "READY=1"
	sdStopping = "STOPPING=1"
	sdWatchdog = "WATCHDOG=1"
)

// notify sends a state string to systemd via the NOTIFY_SOCKET Unix datagram
// socket. It is a no-op when NOTIFY_SOCKET is not set (manual runs, tests).
// A failed notify must never crash the daemon — callers typically log and
// continue.
func notify(state string) error {
	sock := os.Getenv("NOTIFY_SOCKET")
	if sock == "" {
		return nil
	}

	conn, err := net.Dial("unixgram", sock)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Write([]byte(state))
	return err
}

// NotifyReady sends READY=1 to systemd, signalling that the daemon has finished
// startup and its socket is serving. No-op when NOTIFY_SOCKET is unset.
func NotifyReady() error { return notify(sdReady) }

// NotifyStopping sends STOPPING=1 to systemd, signalling that the daemon has
// begun a graceful shutdown. No-op when NOTIFY_SOCKET is unset.
func NotifyStopping() error { return notify(sdStopping) }

// WatchdogInterval reports the systemd watchdog interval for this process and
// whether the watchdog is enabled, mirroring sd_watchdog_enabled(3). It reads
// WATCHDOG_USEC and, when WATCHDOG_PID is set, honours it so a value inherited
// by a child process is ignored. When the watchdog is not enabled for this
// process it returns (0, false), so callers can branch without env parsing.
func WatchdogInterval() (time.Duration, bool) {
	usec := os.Getenv("WATCHDOG_USEC")
	if usec == "" {
		return 0, false
	}
	if pid := os.Getenv("WATCHDOG_PID"); pid != "" && pid != strconv.Itoa(os.Getpid()) {
		// The watchdog was configured for a different process; not ours.
		return 0, false
	}
	us, err := strconv.ParseInt(usec, 10, 64)
	if err != nil || us <= 0 {
		return 0, false
	}
	return time.Duration(us) * time.Microsecond, true
}

// WatchdogOptions configures the optional systemd watchdog keepalive loop.
type WatchdogOptions struct {
	// Logger, when non-nil, logs watchdog lifecycle and ping failures. When nil
	// the loop is silent (discard logger), consistent with the rest of the kit.
	Logger *slog.Logger

	// IsAlive, when non-nil, gates each keepalive: WATCHDOG=1 is sent only when
	// IsAlive() returns true. When it returns false the ping is withheld, so
	// systemd's WatchdogSec timer eventually fires and restarts the PROCESS.
	//
	// Use this ONLY when a process restart is the correct response to the
	// monitored condition — most cleanly a single-monitor daemon where the
	// process IS the monitor, so a restart has no healthy-monitor collateral.
	// Leave it nil for an unconditional keepalive that guards only against a
	// total process freeze. A multi-monitor daemon should generally leave this
	// nil: withholding pings bounces the whole process and resets every healthy
	// monitor too, which is rarely what you want.
	IsAlive func() bool
}

// Watchdog runs the systemd watchdog keepalive loop until ctx is cancelled.
//
// It is a no-op (returns immediately) when the watchdog is not enabled for this
// process — see WatchdogInterval — so it is always safe to call unconditionally;
// enable it by setting WatchdogSec= in the unit file. When enabled it sends
// WATCHDOG=1 to NOTIFY_SOCKET every interval/2 (the conventional safety margin),
// optionally gated by opts.IsAlive.
//
// This is opt-in: nothing else in the kit calls it. Invoke it (typically in its
// own goroutine) only if you want systemd to kill+restart the daemon when it
// stops pinging. Pair it with Restart=on-failure in the unit.
func Watchdog(ctx context.Context, opts WatchdogOptions) {
	interval, ok := WatchdogInterval()
	if !ok {
		return
	}

	log := loggerOrDiscard(opts.Logger)

	ping := interval / 2
	if ping <= 0 {
		ping = interval
	}

	log.Info("systemd watchdog keepalive enabled",
		"reason", "WatchdogEnabled",
		"interval", interval,
		"ping", ping)

	t := time.NewTicker(ping)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if opts.IsAlive != nil && !opts.IsAlive() {
				// Deliberately withhold the keepalive so systemd restarts us.
				log.Warn("withholding systemd watchdog keepalive — liveness check failed",
					"reason", "WatchdogStarved")
				continue
			}
			if err := notify(sdWatchdog); err != nil {
				log.Error("failed to send systemd watchdog keepalive",
					"error", err,
					"reason", "WatchdogPingFailed")
			}
		}
	}
}
