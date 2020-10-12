package grpcsync

import (
	"sync"
	"sync/atomic"
)

type Event struct {
	fired int32
	c     chan struct{}
	o     sync.Once
}

func (e *Event) Fire() bool {
	ret := false
	e.o.Do(func() {
		atomic.StoreInt32(&e.fired, 1)
		close(e.c)
		ret = true
	})
	return ret
}

func (e *Event) Done() <-chan struct{} {
	return e.c
}

func (e *Event) HasFired() bool {
	return atomic.LoadInt32(&e.fired) == 1
}

func NewEvent() *Event {
	return &Event{c: make(chan struct{})}
}
