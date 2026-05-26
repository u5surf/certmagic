package certmagic

import (
	"io"
	stdlog "log"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestJobManagerCleansUpAfterJobPanic verifies that when a submitted job
// panics, the worker still releases the in-flight name and decrements its
// active-worker counter. Without these cleanups, a single panic would
// silently strand all future renewals for that name (and, after enough
// panics, every name) until process restart. See certmagic issue for
// caddyserver/caddy#7366.
func TestJobManagerCleansUpAfterJobPanic(t *testing.T) {
	// Suppress the worker's "panic: certificate worker: ..." message so it
	// doesn't pollute test output. We're intentionally triggering a panic.
	stdlog.SetOutput(io.Discard)
	t.Cleanup(func() { stdlog.SetOutput(io.Discard) })

	jm := &jobManager{maxConcurrentJobs: 10}
	logger := zap.NewNop()

	jm.Submit(logger, "renewal_X", func() error {
		panic("simulated panic from acme library")
	})

	// Cleanup happens in deferred handlers inside worker(), so we cannot
	// synchronize on it from inside the job itself. Poll until state settles.
	if !waitUntil(time.Second, func() bool {
		jm.mu.Lock()
		defer jm.mu.Unlock()
		_, nameStillTracked := jm.names["renewal_X"]
		return !nameStillTracked && jm.activeWorkers == 0
	}) {
		jm.mu.Lock()
		_, nameStillTracked := jm.names["renewal_X"]
		active := jm.activeWorkers
		jm.mu.Unlock()
		t.Fatalf("worker did not clean up after panic: name still tracked=%v, activeWorkers=%d (want false, 0)",
			nameStillTracked, active)
	}

	// A subsequent submission with the same name must actually run.
	// If the names leak regressed, this Submit would be silently dropped.
	var ran int32
	jm.Submit(logger, "renewal_X", func() error {
		atomic.StoreInt32(&ran, 1)
		return nil
	})
	if !waitUntil(time.Second, func() bool {
		return atomic.LoadInt32(&ran) == 1
	}) {
		t.Fatal("second Submit with the same name was silently dropped after panic")
	}
}

func waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}
