package transport

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
)

type writeQuota struct {
	// 配额
	quota int32
	// 有配额可用，则通过此chan通知等待配额的g
	ch chan struct{}
	// 结束chan
	done <-chan struct{}
	// 返还配额
	replenish func(n int)
}

func newWriteQuota(sz int32, done <-chan struct{}) *writeQuota {
	w := &writeQuota{
		quota: sz,
		ch:    make(chan struct{}, 1),
		done:  done,
	}
	w.replenish = w.realReplenish
	return w
}

// 获取sz配额，若没有则阻塞等待
func (w *writeQuota) get(sz int32) error {
	for {
		if atomic.LoadInt32(&w.quota) > 0 {
			atomic.AddInt32(&w.quota, -sz)
			return nil
		}
		select {
		case <-w.ch:
			continue
		case <-w.done:
			return errStreamDone
		}
	}
}

func (w *writeQuota) realReplenish(n int) {
	sz := int32(n)
	a := atomic.AddInt32(&w.quota, sz)
	b := a - sz
	// b<=0保证此时肯定有阻塞的g，才需要往w.ch里面写入chan
	if b <= 0 && a > 0 {
		select {
		case w.ch <- struct{}{}:
		default:
		}
	}
}

type trInFlow struct {
	limit               uint32
	unacked             uint32
	effectiveWindowSize uint32
}

func (f *trInFlow) newLimit(n uint32) uint32 {
	d := n - f.limit
	f.limit = n
	f.updateEffectiveWindowSize()
	return d
}

func (f *trInFlow) onData(n uint32) uint32 {
	f.unacked += n
	if f.unacked >= f.limit/4 {
		w := f.unacked
		f.unacked = 0
		f.updateEffectiveWindowSize()
		return w
	}
	f.updateEffectiveWindowSize()
	return 0
}

func (f *trInFlow) reset() uint32 {
	w := f.unacked
	f.unacked = 0
	f.updateEffectiveWindowSize()
	return w
}

func (f *trInFlow) updateEffectiveWindowSize() {
	atomic.StoreUint32(&f.effectiveWindowSize, f.limit-f.unacked)
}

func (f *trInFlow) getSize() uint32 {
	return atomic.LoadUint32(&f.effectiveWindowSize)
}

type inFlow struct {
	mu            sync.Mutex // 锁
	limit         uint32     // 已经接受但是还没有发送响应的的数据
	pendingData   uint32     // 收到的数据，但是还没有处理
	pendingUpdate uint32     // app已经消费但是还没有发送更新窗口，用以降低更新频率
	delta         uint32     // 当接受者接受的数据大于limit需要此参数
}

// 更新limit，假设n永远大于limit并返回差值
func (f *inFlow) newLimit(n uint32) uint32 {
	f.mu.Lock()
	d := n - f.limit
	f.limit = n
	f.mu.Unlock()
	return d
}

func (f *inFlow) maybeAdjust(n uint32) uint32 {
	if n > uint32(math.MaxInt32) {
		n = uint32(math.MaxInt32)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	estSenderQuota := int32(f.limit - (f.pendingData + f.pendingUpdate)) // 所有还未发送窗口更新的数据(包括处于pending状态的数据)
	// estUntransmittedData is the maximum number of bytes the sends might not have put
	// on the wire yet. A value of 0 or less means that we have already received all or
	// more bytes than the application is requesting to read.
	estUntransmittedData := int32(n - f.pendingData) // Casting into int32 since it could be negative.
	// This implies that unless we send a window update, the sender won't be able to send all the bytes
	// for this message. Therefore we must send an update over the limit since there's an active read
	// request from the application.
	if estUntransmittedData > estSenderQuota {
		// Sender's window shouldn't go more than 2^31 - 1 as specified in the HTTP spec.
		if f.limit+n > maxWindowSize {
			f.delta = maxWindowSize - f.limit
		} else {
			// Send a window update for the whole message and not just the difference between
			// estUntransmittedData and estSenderQuota. This will be helpful in case the message
			// is padded; We will fallback on the current available window(at least a 1/4th of the limit).
			f.delta = n
		}
		return f.delta
	}
	return 0
}

// 接收到数据，则调用onData函数
func (f *inFlow) onData(n uint32) error {
	f.mu.Lock()
	f.pendingData += n
	if f.pendingData+f.pendingUpdate > f.limit+f.delta {
		limit := f.limit
		r := f.pendingData + f.pendingUpdate
		f.mu.Unlock()
		return fmt.Errorf("received %d-bytes data exceeding the limit %d bytes", r, limit)
	}
	f.mu.Unlock()
	return nil
}

// 当应用读取走数据调用onRead函数
func (f *inFlow) onRead(n uint32) uint32 {
	f.mu.Lock()
	if f.pendingData == 0 {
		f.mu.Unlock()
		return 0
	}
	f.pendingData -= n
	if n > f.delta {
		n -= f.delta
		f.delta = 0
	} else {
		f.delta -= n
		n = 0
	}
	f.pendingUpdate += n
	if f.pendingUpdate >= f.limit/4 {
		wu := f.pendingUpdate
		f.pendingUpdate = 0
		f.mu.Unlock()
		return wu
	}
	f.mu.Unlock()
	return 0
}
