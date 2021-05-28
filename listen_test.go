// Copyright 2021 RetailNext, Inc. All rights reserved.
// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package overflow

import (
	"net"
	"runtime"
	"testing"
	"time"
)

const defaultMaxOpenFiles = 256

type testScenario struct {
	clientOptions testClientOptions
	serverLimit   int

	expectBreach bool
	useLimiter   bool
	useMetrics   bool

	logf   func(format string, args ...interface{})
	fatalf func(format string, args ...interface{})
}

type listenerMetrics struct {
	rejected testMetric
	accepted testMetric
	active   testMetric
}

func (s testScenario) makeListener() (net.Listener, *listenerMetrics, error) {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, nil, err
	}

	if !s.useLimiter {
		return listener, nil, nil
	}

	opts := LimitListenerOptions{}
	if s.clientOptions.expectRejectionMessage != "" {
		opts.RejectionHandler = func(conn net.Conn) {
			_, _ = conn.Write([]byte(s.clientOptions.expectRejectionMessage))
			_ = conn.Close()
		}
	}

	var metrics *listenerMetrics
	if s.useMetrics {
		metrics := &listenerMetrics{}
		opts.RejectedConnections = &metrics.rejected
		opts.AcceptedConnections = &metrics.accepted
		opts.ActiveConnections = &metrics.active
	}

	return LimitListener(listener, s.serverLimit, opts), metrics, nil
}

func (s testScenario) run() {
	if s.clientOptions.concurrentAttempts <= s.serverLimit {
		panic("useless testScenario")
	}

	listener, metrics, err := s.makeListener()
	if err != nil {
		panic(err)
	}
	defer func() {
		// This _should_ be closed by server.stop()
		_ = listener.Close()
	}()

	server := testServer{
		l:      listener,
		limit:  int32(s.serverLimit),
		logf:   s.logf,
		fatalf: s.fatalf,
	}
	server.start(runtime.NumCPU() * 3 / 2)

	client := testClient{
		serverAddr: listener.Addr().String(),
		opt:        s.clientOptions,
		logf:       s.logf,
		fatalf:     s.fatalf,
	}
	client.start()
	maxConcurrent := client.finish()

	server.stop()

	clientBreached := maxConcurrent > s.serverLimit
	ok := s.expectBreach == (server.breached || clientBreached)
	if server.breached != clientBreached {
		ok = false
		s.logf("breach_disagree!")
	}
	if int(client.rejectedSessions)+int(client.successfulSessions) != client.opt.attempts {
		s.logf("unaccounted_attempts!")
		ok = false
	}
	if maxConcurrent < s.serverLimit {
		// Possibly a flake on a really under-performing system?
		s.logf("failed_to_reach_limit!")
		ok = false
	}

	if metrics != nil {
		if int(metrics.active.max) != maxConcurrent {
			ok = false
			s.logf("metrics_max_concurrent_disagree!")
		}
		if metrics.active.value != 0 {
			ok = false
			s.logf("metrics_active_not_zero!")
		}
		if metrics.rejected.value != client.rejectedSessions {
			ok = false
			s.logf("metrics_rejected_disagree!")
		}
		if metrics.active.value != client.successfulSessions {
			ok = false
			s.logf("metrics_accepted_disagree!")
		}
	}

	if !ok {

		s.logf("limit %d", s.serverLimit)
		s.logf("client_max_concurrent %d", maxConcurrent)
		s.logf("client_attempts %d", client.opt.attempts)
		s.logf("client_ok_sessions %d", client.successfulSessions)
		s.logf("client_rejected_sessions %d", client.rejectedSessions)
		if metrics != nil {
			s.logf("metrics_accepted value=%d", metrics.accepted.value)
			s.logf("metrics_rejected value=%d", metrics.rejected.value)
			s.logf("metrics_active value=%d max=%d", metrics.active.value, metrics.active.max)
		}
		s.fatalf("case_failed expected_breach=%v server_breached=%v client_breached=%v", s.expectBreach, server.breached, clientBreached)
	}
}

func TestLimitListener(t *testing.T) {
	// Ensure some overhead for fds unrelated to the test.
	effectiveMaxOpenFiles := maxOpenFiles() - 32
	if effectiveMaxOpenFiles < 128 {
		panic("rlimit too low for test to be useful")
	}
	concurrentAttempts := effectiveMaxOpenFiles / 2
	// We don't want to hit accept queue limits
	maxConcurrentAttempts := 128
	if concurrentAttempts > maxConcurrentAttempts {
		concurrentAttempts = maxConcurrentAttempts
	}
	serverLimit := (concurrentAttempts * 3) / 5

	defaults := testScenario{
		clientOptions: testClientOptions{
			baseHold:           100 * time.Millisecond,
			holdStep:           20 * time.Millisecond,
			maxHoldSteps:       10,
			attempts:           concurrentAttempts * 10,
			concurrentAttempts: concurrentAttempts,
		},
		serverLimit: serverLimit,
		logf:        t.Logf,
		fatalf:      t.Fatalf,
	}

	t.Run("Unlimited", func(t *testing.T) {
		ts := defaults
		ts.useLimiter = false
		ts.expectBreach = true
		ts.run()
	})

	t.Run("DefaultRejection", func(t *testing.T) {
		ts := defaults
		ts.useLimiter = true
		ts.expectBreach = false
		// This scenario runs VERY fast.
		ts.clientOptions.attempts = concurrentAttempts * 100
		ts.run()
	})

	t.Run("CustomRejection", func(t *testing.T) {
		ts := defaults
		ts.useLimiter = true
		ts.expectBreach = false
		// This scenario runs faster than Unlimited.
		ts.clientOptions.attempts = concurrentAttempts * 30
		ts.clientOptions.expectRejectionMessage = "REJECTED\n"
		ts.run()
	})

	// Test with and without metrics because the simple metrics implementation adds synchronization.

	t.Run("DefaultRejectionWithMetrics", func(t *testing.T) {
		ts := defaults
		ts.useLimiter = true
		ts.useMetrics = true
		ts.expectBreach = false
		// This scenario runs VERY fast.
		ts.clientOptions.attempts = concurrentAttempts * 50
		ts.run()
	})

	t.Run("CustomRejectionWithMetrics", func(t *testing.T) {
		ts := defaults
		ts.useLimiter = true
		ts.expectBreach = false
		ts.useMetrics = true
		// This scenario runs faster than Unlimited.
		ts.clientOptions.attempts = concurrentAttempts * 30
		ts.clientOptions.expectRejectionMessage = "REJECTED\n"
		ts.run()
	})
}
