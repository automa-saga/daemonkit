// SPDX-License-Identifier: Apache-2.0

//go:build !integration

package daemonkit

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tempSockPath returns a short unix socket path that fits within the
// platform limit (~104 chars on macOS, 108 on Linux). t.TempDir() appends
// the full test function name and can exceed the limit on macOS.
func tempSockPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "sd")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "n.sock")
}

func Test_NotifyReady_NoopWhenSocketUnset(t *testing.T) {
	t.Setenv("NOTIFY_SOCKET", "")
	err := NotifyReady()
	assert.NoError(t, err)
}

func Test_NotifyReady_WritesToSocket(t *testing.T) {
	sockPath := tempSockPath(t)

	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: sockPath, Net: "unixgram"})
	require.NoError(t, err)
	defer conn.Close()

	t.Setenv("NOTIFY_SOCKET", sockPath)

	err = NotifyReady()
	require.NoError(t, err)

	buf := make([]byte, 64)
	n, err := conn.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, sdReady, string(buf[:n]))
}

func Test_NotifyStopping_WritesPayload(t *testing.T) {
	sockPath := tempSockPath(t)

	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: sockPath, Net: "unixgram"})
	require.NoError(t, err)
	defer conn.Close()

	t.Setenv("NOTIFY_SOCKET", sockPath)

	err = NotifyStopping()
	require.NoError(t, err)

	buf := make([]byte, 64)
	n, readErr := conn.Read(buf)
	require.NoError(t, readErr)
	assert.Equal(t, sdStopping, string(buf[:n]))
}

// Verify that notify is a no-op when env var is empty string (not just unset).
func Test_NotifyReady_NoopWhenSocketEmpty(t *testing.T) {
	t.Setenv("NOTIFY_SOCKET", "")
	err := NotifyReady()
	assert.NoError(t, err)
}

func Test_WatchdogInterval(t *testing.T) {
	t.Run("disabled when unset", func(t *testing.T) {
		t.Setenv("WATCHDOG_USEC", "")
		t.Setenv("WATCHDOG_PID", "")
		_, ok := WatchdogInterval()
		assert.False(t, ok)
	})

	t.Run("enabled with valid usec", func(t *testing.T) {
		t.Setenv("WATCHDOG_USEC", "30000000")
		t.Setenv("WATCHDOG_PID", "")
		d, ok := WatchdogInterval()
		require.True(t, ok)
		assert.Equal(t, 30*time.Second, d)
	})

	t.Run("disabled when PID names another process", func(t *testing.T) {
		t.Setenv("WATCHDOG_USEC", "30000000")
		t.Setenv("WATCHDOG_PID", strconv.Itoa(os.Getpid()+1))
		_, ok := WatchdogInterval()
		assert.False(t, ok)
	})

	t.Run("enabled when PID matches us", func(t *testing.T) {
		t.Setenv("WATCHDOG_USEC", "30000000")
		t.Setenv("WATCHDOG_PID", strconv.Itoa(os.Getpid()))
		_, ok := WatchdogInterval()
		assert.True(t, ok)
	})

	t.Run("disabled when usec is not a positive integer", func(t *testing.T) {
		t.Setenv("WATCHDOG_USEC", "garbage")
		t.Setenv("WATCHDOG_PID", "")
		_, ok := WatchdogInterval()
		assert.False(t, ok)
	})
}

func Test_Watchdog_NoopWhenDisabled(t *testing.T) {
	t.Setenv("WATCHDOG_USEC", "")
	// Must return immediately even with a never-cancelled context.
	done := make(chan struct{})
	go func() {
		Watchdog(context.Background(), WatchdogOptions{})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Watchdog did not return when the watchdog is disabled")
	}
}

func Test_Watchdog_PingsSocket(t *testing.T) {
	sockPath := tempSockPath(t)
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: sockPath, Net: "unixgram"})
	require.NoError(t, err)
	defer conn.Close()

	t.Setenv("NOTIFY_SOCKET", sockPath)
	// 20ms interval → ~10ms ping cadence; fast enough for the test.
	t.Setenv("WATCHDOG_USEC", "20000")
	t.Setenv("WATCHDOG_PID", "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go Watchdog(ctx, WatchdogOptions{})

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	buf := make([]byte, 64)
	n, readErr := conn.Read(buf)
	require.NoError(t, readErr)
	assert.Equal(t, sdWatchdog, string(buf[:n]))
}

func Test_Watchdog_IsAliveGatesPing(t *testing.T) {
	sockPath := tempSockPath(t)
	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: sockPath, Net: "unixgram"})
	require.NoError(t, err)
	defer conn.Close()

	t.Setenv("NOTIFY_SOCKET", sockPath)
	t.Setenv("WATCHDOG_USEC", "20000")
	t.Setenv("WATCHDOG_PID", "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// IsAlive always false → no ping should ever reach the socket.
	go Watchdog(ctx, WatchdogOptions{IsAlive: func() bool { return false }})

	require.NoError(t, conn.SetReadDeadline(time.Now().Add(300*time.Millisecond)))
	buf := make([]byte, 64)
	_, readErr := conn.Read(buf)
	assert.Error(t, readErr, "no keepalive must be sent while IsAlive reports false")
}
