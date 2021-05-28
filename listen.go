// Copyright 2021 RetailNext, Inc. All rights reserved.
// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package overflow

import (
	"net"
	"sync"
)

// LimitListener returns a Listener that accepts at most n simultaneous
// connections from the provided Listener.
func LimitListener(l net.Listener, n int, options LimitListenerOptions) net.Listener {
	return &limitListener{
		Listener: l,
		sem:      make(chan struct{}, n),
		options:  options,
	}
}

type LimitListenerOptions struct {
	// RejectionHandler is used to turn away connections while the limit is reached.
	// If RejectionHandler is nil, the default behavior is that LimitListener will immediately close the connection.
	// If RejectionHandler is not nil, it is responsible for closing the rejected connection.
	// RejectionHandler is called in the goroutine that called LimitListener.Accept.
	RejectionHandler func(net.Conn)

	// RejectedConnections is incremented each time a connection is rejected due to the limit being reached.
	RejectedConnections Counter

	// AcceptedConnections is incremented each time a connection is returned to the Accept() caller.
	AcceptedConnections Counter

	// ActiveConnections is incremented each time a connection is returned to the Accept() caller,
	// and decremented when the connection is closed.
	ActiveConnections Gauge
}

// Counter should be a prometheus.Counter or equivalent.
type Counter interface {
	Inc()
}

// Gauge should be a prometheus.Gauge or equivalent.
type Gauge interface {
	Inc()
	Dec()
}

type limitListener struct {
	net.Listener
	sem     chan struct{}
	options LimitListenerOptions
}

func (l *limitListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}

		select {
		case l.sem <- struct{}{}:
			return l.acquired(c)
		default:
			// The semaphore was not acquired, so reject the connection and Accept() again via the loop.
			l.reject(c)
		}
	}
}

func (l *limitListener) acquired(c net.Conn) (net.Conn, error) {
	if l.options.AcceptedConnections != nil {
		l.options.AcceptedConnections.Inc()
	}
	if l.options.ActiveConnections != nil {
		l.options.ActiveConnections.Inc()
	}
	return &limitListenerConn{Conn: c, release: l.release}, nil
}

func (l *limitListener) release() {
	<-l.sem
	if l.options.ActiveConnections != nil {
		l.options.ActiveConnections.Dec()
	}
}

func (l *limitListener) reject(c net.Conn) {
	if l.options.RejectionHandler != nil {
		l.options.RejectionHandler(c)
	} else {
		_ = c.Close()
	}
	if l.options.RejectedConnections != nil {
		l.options.RejectedConnections.Inc()
	}
}

type limitListenerConn struct {
	net.Conn
	releaseOnce sync.Once
	release     func()
}

func (l *limitListenerConn) Close() error {
	err := l.Conn.Close()
	l.releaseOnce.Do(l.release)
	return err
}
