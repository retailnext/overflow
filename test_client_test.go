// Copyright 2021 RetailNext, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package overflow

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type testClientOptions struct {
	baseHold     time.Duration
	holdStep     time.Duration
	maxHoldSteps int

	attempts           int
	concurrentAttempts int

	expectRejectionMessage string
}

type testClient struct {
	serverAddr string
	opt        testClientOptions
	logf       func(format string, args ...interface{})
	fatalf     func(format string, args ...interface{})

	rejectedSessions   int32
	successfulSessions int32

	intervals     intervalLog
	intervalsLock sync.Mutex

	limit     chan struct{}
	sessionWg sync.WaitGroup
}

func (cl *testClient) start() {
	cl.intervals = intervalLog{}
	cl.limit = make(chan struct{}, cl.opt.concurrentAttempts)
	for i := 0; i < cl.opt.attempts; i++ {
		cl.runAttempt(i)
	}
}

func isHangup(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, io.EOF) || strings.Contains(err.Error(), "reset by peer")
}

func waitForHangup(reader *bufio.Reader) {
	for {
		_, err := reader.ReadString('\n')
		if err != nil {
			if isHangup(err) {
				return
			}
			panic(fmt.Errorf("unexpected non-hangup error: %+v", err))
		}
	}
}

func (cl *testClient) runAttempt(attemptID int) {
	cl.sessionWg.Add(1)
	cl.limit <- struct{}{}
	go func() {
		defer func() {
			<-cl.limit
			cl.sessionWg.Done()
		}()
		conn, err := net.Dial("tcp", cl.serverAddr)
		if err != nil {
			// This should not happen at all. If there are dialing errors, some OS limit is being reached.
			cl.fatalf("client_dial_error attemptID=%d err=%+v", attemptID, err)
			return
		}

		var intervalBegin, intervalEnd time.Time
		var wasSuccessful, wasRejected bool

		defer func() {
			_ = conn.Close()
			if wasSuccessful {
				cl.intervalsLock.Lock()
				cl.intervals.add(intervalBegin, intervalEnd)
				cl.intervalsLock.Unlock()
				atomic.AddInt32(&cl.successfulSessions, 1)
			} else if wasRejected {
				atomic.AddInt32(&cl.rejectedSessions, 1)
			}
		}()

		reader := bufio.NewReader(conn)
		resp, err := reader.ReadString('\n')
		if resp == "HELLO\n" {
			if err != nil {
				// Should never error and succeed at the same time
				cl.fatalf("client_read_hello_failed attemptID=%d err=%+v", attemptID, err)
				return
			}
			// Continues on to hold below.
		} else if cl.opt.expectRejectionMessage != "" {
			if resp == cl.opt.expectRejectionMessage {
				//				cl.logf("got_rejection_message attemptID=%d", attemptID)
				wasRejected = true
				if err != nil {
					if isHangup(err) {
						return
					}
					cl.fatalf("client_read_hello_failed attemptID=%d err=%+v", attemptID, err)
					return
				}
				waitForHangup(reader)
			} else {
				// Just an error, or wrong message.
				// We should not get a plain hangup when expecting a rejection.
				cl.fatalf("client_read_hello_failed attemptID=%d resp=%q err=%+v", attemptID, resp, err)
			}
			return
		} else {
			if err != nil {
				if !isHangup(err) {
					// Should never get a non-hangup error
					cl.fatalf("client_read_hello_failed attemptID=%d err=%+v", attemptID, err)
				}
			}
			// Not expecting a rejection message, treat hangups as rejections.
			wasRejected = true
			waitForHangup(reader)
			return
		}

		wasSuccessful = true
		intervalBegin = time.Now()
		// Keep the connection open a varying duration based on our attemptID
		variance := time.Duration(attemptID%cl.opt.maxHoldSteps) * cl.opt.holdStep
		time.Sleep(cl.opt.baseHold + variance)
		intervalEnd = time.Now()

		// For half the connections, hang up instead of asking the server to close.
		if attemptID%2 == 0 {
			return
		}

		if _, err := conn.Write([]byte("X\n")); err != nil {
			cl.logf("client_write_string_error attemptID=%d err=%+v", attemptID, err)
			return
		}
		waitForHangup(reader)
	}()
}

func (cl *testClient) finish() (maxConcurrent int) {
	cl.sessionWg.Wait()
	cl.intervalsLock.Lock()
	defer cl.intervalsLock.Unlock()
	return cl.intervals.maxConcurrent()
}
