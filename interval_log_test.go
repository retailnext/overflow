// Copyright 2021 RetailNext, Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package overflow

import (
	"sort"
	"testing"
	"time"
)

type intervalLog []intervalLogEntry

type intervalLogEntry struct {
	time.Time
	delta int
}

func (s *intervalLog) add(t0, t1 time.Time) {
	*s = append(*s, intervalLogEntry{t0, 1}, intervalLogEntry{t1, -1})
}

func (s intervalLog) Len() int {
	return len(s)
}

func (s intervalLog) Less(i, j int) bool {
	return s[i].Before(s[j].Time)
}

func (s intervalLog) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s intervalLog) maxConcurrent() int {
	sort.Sort(s)
	var cur, result int
	for _, entry := range s {
		cur += entry.delta
		if cur > result {
			result = cur
		}
	}
	return result
}

func TestIntervalLog(t *testing.T) {
	t0 := time.Now().Add(-time.Hour)
	t1 := t0.Add(time.Minute)
	t2 := t1.Add(time.Minute)
	t3 := t2.Add(time.Minute)
	t4 := t3.Add(time.Minute)
	t5 := t4.Add(time.Minute)
	t6 := t5.Add(time.Minute)
	il := intervalLog{}
	il.add(t0, t3)
	il.add(t1, t4)
	il.add(t2, t3)
	il.add(t5, t6)
	result := il.maxConcurrent()
	if il.maxConcurrent() != 3 {
		t.Fatalf("wrong_result got=%d", result)
	}
}
