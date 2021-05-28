// Copyright 2021 RetailNext, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package overflow

import (
	"sync"
	"sync/atomic"
)

type testMetric struct {
	value int32
	max   int32
	lock  sync.Mutex
}

func (m *testMetric) Inc() {
	atomic.AddInt32(&m.value, 1)
}

func (m *testMetric) Dec() {
	m.lock.Lock()
	defer m.lock.Unlock()
	was := atomic.AddInt32(&m.value, -1) + 1
	if was > m.max {
		m.max = was
	}
}
