// Copyright 2021 RetailNext, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package overflow

import (
	"bufio"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

type testServer struct {
	l net.Listener

	stopCh chan struct{}

	acceptWg sync.WaitGroup
	handleWg sync.WaitGroup

	limit    int32
	active   int32
	breached bool

	logf   func(format string, args ...interface{})
	fatalf func(format string, args ...interface{})
}

func (svc *testServer) stop() {
	close(svc.stopCh)
	if err := svc.l.Close(); err != nil {
		svc.fatalf("server_listener_close_failed err=%+v", err)
	}
	svc.acceptWg.Wait()
	svc.handleWg.Wait()
}

func (svc *testServer) start(acceptRoutines int) {
	svc.stopCh = make(chan struct{})
	for n := 0; n < acceptRoutines; n++ {
		svc.accept()
	}
}

func (svc *testServer) accept() {
	svc.acceptWg.Add(1)
	go func() {
		defer svc.acceptWg.Done()
		for {
			conn, err := svc.l.Accept()
			if err != nil {
				return
			}
			svc.handleConn(conn)
		}
	}()
}

func (svc *testServer) handleConn(c net.Conn) {
	active := atomic.AddInt32(&svc.active, 1)
	if active > svc.limit {
		svc.breached = true
	}
	svc.handleWg.Add(1)
	go func() {
		defer func() {
			atomic.AddInt32(&svc.active, -1)
			_ = c.Close()
			svc.handleWg.Done()
		}()
		if _, err := c.Write([]byte("HELLO\n")); err != nil {
			svc.logf("server_write_string_failed err=%+v", err)
			return
		}
		reader := bufio.NewReader(c)
		if _, err := reader.ReadString('\n'); err != nil {
			if !errors.Is(err, io.EOF) {
				svc.logf("server_read_string_failed err=%+v", err)
			}
			return
		}
		if _, err := c.Write([]byte("GOODBYE\n")); err != nil {
			if !errors.Is(err, io.EOF) {
				svc.logf("server_write_string_failed err=%+v", err)
			}
		}
	}()
}
