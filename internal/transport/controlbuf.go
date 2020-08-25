package transport

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

var updateHeaderTblSize = func(e *hpack.Encoder, v uint32) {
	e.SetMaxDynamicTableSizeLimit(v)
}

type side int

const (
	clientSide side = iota
	serverSide
)

type outStreamState int

const (
	active outStreamState = iota
	empty
	waitingOnStreamQuota
)

const minBatchSize = 1000

type itemNode struct {
	it   interface{}
	next *itemNode
}

type itemList struct {
	head *itemNode
	tail *itemNode
}

// 追加到元素尾部
func (il *itemList) enqueue(i interface{}) {
	n := &itemNode{it: i}
	if il.tail == nil {
		il.head, il.tail = n, n
		return
	}
	il.tail.next = n
	il.tail = n
}

// 取出不删除第一个元素
func (il *itemList) peek() interface{} {
	return il.head.it
}

// 取出并删除头元素
func (il *itemList) dequeue() interface{} {
	if il.head == nil {
		return nil
	}
	i := il.head.it
	il.head = il.head.next
	if il.head == nil {
		il.tail = nil
	}
	return i
}

// 取出并删除整个列表
func (il *itemList) dequeueAll() *itemNode {
	h := il.head
	il.head, il.tail = nil, nil
	return h
}

// 列表是否为空
func (il *itemList) isEmpty() bool {
	return il.head == nil
}

// 同时能够发送的最大响应stream帧的数量
const maxQueuedTransportResponseFrames = 50

type cbItem interface {
	isTransportResponseFrame() bool
}

// 注册一个incoming stream到loopy writer
type registerStream struct {
	streamID uint32
	wq       *writeQuota
}

func (*registerStream) isTransportResponseFrame() bool { return false }

// 在client-side，此frame也用于注册
// 服务端发送客户端响应的header帧
type headerFrame struct {
	streamID   uint32 // 流id
	hf         []hpack.HeaderField
	endStream  bool               // Valid on server side.
	initStream func(uint32) error // Used only on the client side.
	onWrite    func()
	wq         *writeQuota    // write quota for the stream created.
	cleanup    *cleanupStream // Valid on the server side.
	onOrphaned func(error)    // Valid on client-side
}

func (h *headerFrame) isTransportResponseFrame() bool {
	return h.cleanup != nil && h.cleanup.rst
}

type cleanupStream struct {
	streamID uint32
	rst      bool
	rstCode  http2.ErrCode
	onWrite  func()
}

func (c *cleanupStream) isTransportResponseFrame() bool { return c.rst } // Results in a RST_STREAM

// 数据帧
type dataFrame struct {
	streamID    uint32
	endStream   bool
	h           []byte
	d           []byte
	onEachWrite func()
}

func (*dataFrame) isTransportResponseFrame() bool { return false }

// 窗口更新帧
type incomingWindowUpdate struct {
	streamID  uint32
	increment uint32
}

func (*incomingWindowUpdate) isTransportResponseFrame() bool { return false }

type outgoingWindowUpdate struct {
	streamID  uint32
	increment uint32
}

func (*outgoingWindowUpdate) isTransportResponseFrame() bool {
	return false // window updates are throttled by thresholds
}

// setting帧
type incomingSettings struct {
	ss []http2.Setting
}

func (*incomingSettings) isTransportResponseFrame() bool { return true } // Results in a settings ACK

type outgoingSettings struct {
	ss []http2.Setting
}

func (*outgoingSettings) isTransportResponseFrame() bool { return false }

type incomingGoAway struct {
}

func (*incomingGoAway) isTransportResponseFrame() bool { return false }

type goAway struct {
	code      http2.ErrCode
	debugData []byte
	headsUp   bool
	closeConn bool
}

func (*goAway) isTransportResponseFrame() bool { return false }

type ping struct {
	ack  bool
	data [8]byte
}

func (*ping) isTransportResponseFrame() bool { return true }

type outFlowControlSizeRequest struct {
	resp chan uint32
}

func (*outFlowControlSizeRequest) isTransportResponseFrame() bool { return false }

// stream存储那些要发出数据的stream
type outStream struct {
	id               uint32         // 流id
	state            outStreamState // 流状态
	itl              *itemList      // frame
	bytesOutStanding int
	wq               *writeQuota
	next             *outStream
	prev             *outStream
}

func (s *outStream) deleteSelf() {
	if s.prev != nil {
		s.prev.next = s.next
	}
	if s.next != nil {
		s.next.prev = s.prev
	}
	s.next, s.prev = nil, nil
}

type outStreamList struct {
	head *outStream
	tail *outStream
}

func newOutStreamList() *outStreamList {
	head, tail := new(outStream), new(outStream)
	head.next = tail
	tail.prev = head
	return &outStreamList{
		head: head,
		tail: tail,
	}
}

func (l *outStreamList) enqueue(s *outStream) {
	e := l.tail.prev
	e.next = s
	s.prev = e
	s.next = l.tail
	l.tail.prev = s
}

func (l *outStreamList) dequeue() *outStream {
	b := l.head.next
	if b == l.tail {
		return nil
	}
	b.deleteSelf()
	return b
}

type controlBuffer struct {
	ch                      chan struct{}   // 通过此ch唤醒等待读取元素的g
	done                    <-chan struct{} // 结束chan
	mu                      sync.Mutex      // 锁
	consumerWaiting         bool            // 是否有消费者在等待
	list                    *itemList       // 元素列表
	err                     error           // 错误信息
	transportResponseFrames int             // 当前响应流的个数
	trfChan                 atomic.Value    // 控制trf
}

// 初始化一个controlBuffer
func newControlBuffer(done <-chan struct{}) *controlBuffer {
	return &controlBuffer{
		ch:   make(chan struct{}, 1),
		list: &itemList{},
		done: done,
	}
}

func (c *controlBuffer) throttle() {
	ch, _ := c.trfChan.Load().(*chan struct{})
	if ch != nil {
		select {
		case <-*ch:
		case <-c.done:
		}
	}
}

func (c *controlBuffer) put(it cbItem) error {
	_, err := c.executeAndPut(nil, it)
	return err
}

// 放入一个元素到controlBuffer
func (c *controlBuffer) executeAndPut(f func(it interface{}) bool, it cbItem) (bool, error) {
	var wakeUp bool
	c.mu.Lock()
	// 错误信息不为空，直接返回
	if c.err != nil {
		c.mu.Unlock()
		return false, c.err
	}
	if f != nil {
		if !f(it) {
			c.mu.Unlock()
			return false, nil
		}
	}
	// 如果有消费者等待，唤醒
	if c.consumerWaiting {
		wakeUp = true
		c.consumerWaiting = false
	}
	c.list.enqueue(it)
	if it.isTransportResponseFrame() {
		c.transportResponseFrames++
		if c.transportResponseFrames == maxQueuedTransportResponseFrames {
			ch := make(chan struct{})
			c.trfChan.Store(&ch)
		}
	}
	c.mu.Unlock()
	// 若wakeUp为true，则向ch写入数据
	if wakeUp {
		select {
		case c.ch <- struct{}{}:
		default:
		}
	}
	return true, nil
}

func (c *controlBuffer) execute(f func(it interface{}) bool, it interface{}) (bool, error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return false, c.err
	}
	if !f(it) {
		c.mu.Unlock()
		return false, nil
	}
	c.mu.Unlock()
	return true, nil
}

// 若block为true，则阻塞直到有新数据插入
func (c *controlBuffer) get(block bool) (interface{}, error) {
	for {
		c.mu.Lock()
		if c.err != nil {
			c.mu.Unlock()
			return nil, c.err
		}
		if !c.list.isEmpty() {
			h := c.list.dequeue().(cbItem)
			if h.isTransportResponseFrame() {
				if c.transportResponseFrames == maxQueuedTransportResponseFrames {
					ch := c.trfChan.Load().(*chan struct{})
					close(*ch)
					c.trfChan.Store((*chan struct{})(nil))
				}
				c.transportResponseFrames--
			}
			c.mu.Unlock()
			return h, nil
		}
		if !block {
			c.mu.Unlock()
			return nil, nil
		}
		// 若此时队列元素为空，则consumerWaiting置为true
		// 同时监听ch和done chan
		c.consumerWaiting = true
		c.mu.Unlock()
		select {
		case <-c.ch:
		case <-c.done:
			c.finish()
			return nil, ErrConnClosing
		}
	}
}

// 若done chan有值，则证明client取消了调用
// 需要将未发送的header帧取消
func (c *controlBuffer) finish() {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.err = ErrConnClosing
	// 清除header流
	for head := c.list.dequeueAll(); head != nil; head = head.next {
		hdr, ok := head.it.(*headerFrame)
		if !ok {
			continue
		}
		// onOrphaned在server-side为nil
		if hdr.onOrphaned != nil {
			hdr.onOrphaned(ErrConnClosing)
		}
	}
	c.mu.Unlock()
}

type loopyWriter struct {
	side      side
	cbuf      *controlBuffer // frame控制器
	sendQuota uint32         // 发送配额(流级别)
	oiws      uint32         // 出方向初始化的窗口大小
	// 表示已经建立连接的stream并且没有被cleaned-up
	// 用作客户端的时候，表示那些头部已经被发出去的stream
	// 用作服务端的时候，表示那些头部已经被收到的stream
	estdStreams     map[uint32]*outStream
	activeStreams   *outStreamList // 活动的stream
	framer          *framer        // 二进制帧控制器
	hBuf            *bytes.Buffer  // 哈夫曼编码的buffer
	hEnc            *hpack.Encoder // 哈夫曼编码器
	bdpEst          *bdpEstimator
	draining        bool
	ssGoAwayHandler func(*goAway) (bool, error)
}

func newLoopyWriter(s side, fr *framer, cbuf *controlBuffer, bdpEst *bdpEstimator) *loopyWriter {
	var buf bytes.Buffer
	l := &loopyWriter{
		side:          s,
		cbuf:          cbuf,
		sendQuota:     defaultWindowSize,
		oiws:          defaultWindowSize,
		estdStreams:   make(map[uint32]*outStream),
		activeStreams: newOutStreamList(),
		framer:        fr,
		hBuf:          &buf,
		hEnc:          hpack.NewEncoder(&buf),
		bdpEst:        bdpEst,
	}
	return l
}

// loopyWriter主循环，主要分为两部操作：
//	  1、处理controlBuffer的frame
//	  2、处理activeStream未发送的数据
func (l *loopyWriter) run() (err error) {
	defer func() {
		if err == ErrConnClosing {
			// 不能将此err当作错误来处理
			// 1. When the connection is closed by some other known issue.
			// 2. User closed the connection.
			// 3. A graceful close of connection.
			if logger.V(logLevel) {
				logger.Infof("transport: loopyWriter.run returning. %v", err)
			}
			err = nil
		}
	}()
	for {
		// 阻塞读取frame
		it, err := l.cbuf.get(true)
		if err != nil {
			return err
		}
		// 处理frame
		if err = l.handle(it); err != nil {
			return err
		}
		// 处理activeStream未发送的数据
		if _, err = l.processData(); err != nil {
			return err
		}
		// 修改schedule和hasData两个参数名称，编译器飘波浪线，看着难受
		schedule := true
	hasData:
		// 内层循环
		// 优化点：若有数据，则get数据不需要阻塞，直接for循环
		// controlBuffer没有数据不代表没有data帧需要处理
		for {
			it, err := l.cbuf.get(false)
			if err != nil {
				return err
			}
			if it != nil {
				if err = l.handle(it); err != nil {
					return err
				}
				if _, err = l.processData(); err != nil {
					return err
				}
				continue hasData
			}
			isEmpty, err := l.processData()
			if err != nil {
				return err
			}
			// 若非空，则继续内层循环
			if !isEmpty {
				continue hasData
			}
			// 否则发起调度，让出cpu给其他g
			// 仅当buffer小于最小的批量发布数据的大小的时候
			if schedule {
				schedule = false
				if l.framer.writer.offset < minBatchSize {
					runtime.Gosched()
					continue hasData
				}
			}
			// 刷新framer缓冲区
			l.framer.writer.Flush()
			break hasData
		}
	}
}

// 根据buf item的类型，选择不同的处理函数
func (l *loopyWriter) handle(i interface{}) error {
	switch i := i.(type) {
	case *incomingWindowUpdate:
		return l.incomingWindowUpdateHandler(i)
	case *outgoingWindowUpdate:
		return l.outgoingWindowUpdateHandler(i)
	case *incomingSettings:
		return l.incomingSettingsHandler(i)
	case *outgoingSettings:
		return l.outgoingSettingsHandler(i)
	case *headerFrame:
		return l.headerHandler(i)
	case *registerStream:
		return l.registerStreamHandler(i)
	case *cleanupStream:
		return l.cleanupStreamHandler(i)
	case *incomingGoAway:
		return l.incomingGoAwayHandler(i)
	case *dataFrame:
		return l.preprocessData(i)
	case *ping:
		return l.pingHandler(i)
	case *goAway:
		return l.goAwayHandler(i)
	case *outFlowControlSizeRequest:
		return l.outFlowControlSizeRequestHandler(i)
	default:
		return fmt.Errorf("transport: unknown control message type %T", i)
	}
}

// 服务器发出的更新窗口
// 告知客户端发送窗口可以增加(默认是16k)
func (l *loopyWriter) outgoingWindowUpdateHandler(w *outgoingWindowUpdate) error {
	return l.framer.fr.WriteWindowUpdate(w.streamID, w.increment)
}

// 客户端发送的更新窗口，通知server可以发送更多的数据
// 主要是限流
func (l *loopyWriter) incomingWindowUpdateHandler(w *incomingWindowUpdate) error {
	// 若stream_id为0，则为连接级别的流控
	if w.streamID == 0 {
		l.sendQuota += w.increment
		return nil
	}
	// 否则为stream级别的流控
	if str, ok := l.estdStreams[w.streamID]; ok {
		// bytesOutStanding(可以理解为stream正在等待回复的字节长度)
		str.bytesOutStanding -= int(w.increment)
		// 若此时配额大于0且流的状态处于等待配额的状态，则激活流
		if strQuota := int(l.oiws) - str.bytesOutStanding; strQuota > 0 && str.state == waitingOnStreamQuota {
			str.state = active
			l.activeStreams.enqueue(str)
			return nil
		}
	}
	return nil
}

// 发送给客户端的setting帧
func (l *loopyWriter) outgoingSettingsHandler(s *outgoingSettings) error {
	return l.framer.fr.WriteSettings(s.ss...)
}

// 客户端发送的setting帧
func (l *loopyWriter) incomingSettingsHandler(s *incomingSettings) error {
	if err := l.applySettings(s.ss); err != nil {
		return err
	}
	return l.framer.fr.WriteSettingsAck()
}

// 注册一个stream到loopyWriter
func (l *loopyWriter) registerStreamHandler(h *registerStream) error {
	str := &outStream{
		id:    h.streamID,
		state: empty,
		itl:   &itemList{},
		wq:    h.wq,
	}
	l.estdStreams[h.streamID] = str
	return nil
}

// 处理头帧
// 对于服务端，若有headerFrame，则是服务端写回数据给客户端
func (l *loopyWriter) headerHandler(h *headerFrame) error {
	if l.side == serverSide {
		str, ok := l.estdStreams[h.streamID]
		if !ok {
			if logger.V(logLevel) {
				logger.Warningf("transport: loopy doesn't recognize the stream: %d", h.streamID)
			}
			return nil
		}
		// 若endStream为false，则server是回应客户端用头帧
		if !h.endStream {
			return l.writeHeader(h.streamID, h.endStream, h.hf, h.onWrite)
		}
		// 若endStream为true，则server是想关闭stream
		if str.state != empty {
			str.itl.enqueue(h)
			return nil
		}
		// 发送给客户端头帧
		if err := l.writeHeader(h.streamID, h.endStream, h.hf, h.onWrite); err != nil {
			return err
		}
		return l.cleanupStreamHandler(h.cleanup)
	}

	// 以下是为客户端准备的，客户端想发起一个stream
	str := &outStream{
		id:    h.streamID,
		state: empty,
		itl:   &itemList{},
		wq:    h.wq,
	}
	str.itl.enqueue(h)
	return l.originateStream(str)
}

// 发起一个stream
func (l *loopyWriter) originateStream(str *outStream) error {
	hdr := str.itl.dequeue().(*headerFrame)
	if err := hdr.initStream(str.id); err != nil {
		if err == ErrConnClosing {
			return err
		}
		// 其他error不需要关闭transport
		return nil
	}
	// 写header
	if err := l.writeHeader(str.id, hdr.endStream, hdr.hf, hdr.onWrite); err != nil {
		return err
	}
	l.estdStreams[str.id] = str
	return nil
}

// 发送header
func (l *loopyWriter) writeHeader(streamID uint32, endStream bool, hf []hpack.HeaderField, onWrite func()) error {
	if onWrite != nil {
		onWrite()
	}
	// 编码header属性
	l.hBuf.Reset()
	for _, f := range hf {
		if err := l.hEnc.WriteField(f); err != nil {
			if logger.V(logLevel) {
				logger.Warningf("transport: loopyWriter.writeHeader encountered error while encoding headers: %v", err)
			}
		}
	}
	var (
		err               error
		endHeaders, first bool
	)
	// 发送头部，若头部超过默认最大长度，
	// 则二次发送Continuation帧
	first = true
	for !endHeaders {
		size := l.hBuf.Len()
		if size > http2MaxFrameLen {
			size = http2MaxFrameLen
		} else {
			endHeaders = true
		}
		if first {
			first = false
			err = l.framer.fr.WriteHeaders(http2.HeadersFrameParam{
				StreamID:      streamID,
				BlockFragment: l.hBuf.Next(size),
				EndStream:     endStream,
				EndHeaders:    endHeaders,
			})
		} else {
			err = l.framer.fr.WriteContinuation(
				streamID,
				endHeaders,
				l.hBuf.Next(size),
			)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// data frame预处理
// 若对应的stream状态为empty，则更改状态为active，并将其放入到activeStreams
func (l *loopyWriter) preprocessData(df *dataFrame) error {
	str, ok := l.estdStreams[df.streamID]
	if !ok {
		return nil
	}
	// 如果我们收到了一个数据帧，则意味着此stream已经被发起且头部已经被发出
	str.itl.enqueue(df)
	if str.state == empty {
		str.state = active
		l.activeStreams.enqueue(str)
	}
	return nil
}

// 处理ping函数
func (l *loopyWriter) pingHandler(p *ping) error {
	if !p.ack {
		l.bdpEst.timesnap(p.data)
	}
	return l.framer.fr.WritePing(p.ack, p.data)
}

// 主要是记录指标用
func (l *loopyWriter) outFlowControlSizeRequestHandler(o *outFlowControlSizeRequest) error {
	o.resp <- l.sendQuota
	return nil
}

// 清理stream函数
func (l *loopyWriter) cleanupStreamHandler(c *cleanupStream) error {
	c.onWrite()
	if str, ok := l.estdStreams[c.streamID]; ok {
		// On the server side it could be a trailers-only response or
		// a RST_STREAM before stream initialization thus the stream might
		// not be established yet.
		delete(l.estdStreams, c.streamID)
		str.deleteSelf()
	}
	// 若需要发送RST_STREAM，则发送
	if c.rst {
		if err := l.framer.fr.WriteRSTStream(c.streamID, c.rstCode); err != nil {
			return err
		}
	}
	if l.side == clientSide && l.draining && len(l.estdStreams) == 0 {
		return ErrConnClosing
	}
	return nil
}

func (l *loopyWriter) incomingGoAwayHandler(*incomingGoAway) error {
	if l.side == clientSide {
		l.draining = true
		if len(l.estdStreams) == 0 {
			return ErrConnClosing
		}
	}
	return nil
}

func (l *loopyWriter) goAwayHandler(g *goAway) error {
	// Handling of outgoing GoAway is very specific to side.
	if l.ssGoAwayHandler != nil {
		draining, err := l.ssGoAwayHandler(g)
		if err != nil {
			return err
		}
		l.draining = draining
	}
	return nil
}

// 应用setting帧
func (l *loopyWriter) applySettings(ss []http2.Setting) error {
	for _, s := range ss {
		switch s.ID {
		case http2.SettingInitialWindowSize:
			o := l.oiws
			l.oiws = s.Val
			if o < l.oiws {
				// 如果窗口大小变大，则激活waitingOnStreamQuota状态的stream
				for _, stream := range l.estdStreams {
					if stream.state == waitingOnStreamQuota {
						stream.state = active
						l.activeStreams.enqueue(stream)
					}
				}
			}
		case http2.SettingHeaderTableSize:
			updateHeaderTblSize(l.hEnc, s.Val)
		}
	}
	return nil
}

// 此函数主要用于处理数据帧
// 先把stream从active streams移除，并最多写16kb的数据
// 若数据还有剩余，则将其加入到activeStreams的尾部，继续等待发送
// @return 若为true，则代表当前stream要发送的数据为空；若为false，则不为空
func (l *loopyWriter) processData() (bool, error) {
	// 如果发送配额已经为0，直接返回
	if l.sendQuota == 0 {
		return true, nil
	}
	// 移除首部的stream
	str := l.activeStreams.dequeue()
	if str == nil {
		return true, nil
	}
	// 获取第一个帧
	dataItem := str.itl.peek().(*dataFrame)
	// 空帧
	if len(dataItem.h) == 0 && len(dataItem.d) == 0 {
		// 客户端发送空帧同时endStream为true
		if err := l.framer.fr.WriteData(dataItem.streamID, dataItem.endStream, nil); err != nil {
			return false, err
		}
		// 移除data frame
		str.itl.dequeue()
		// 若itl为空，则str的状态为空
		if str.itl.isEmpty() {
			str.state = empty
		} else if trailer, ok := str.itl.peek().(*headerFrame); ok {
			// the next item is trailers.
			if err := l.writeHeader(trailer.streamID, trailer.endStream, trailer.hf, trailer.onWrite); err != nil {
				return false, err
			}
			if err := l.cleanupStreamHandler(trailer.cleanup); err != nil {
				return false, nil
			}
		} else {
			l.activeStreams.enqueue(str)
		}
		return false, nil
	}
	var (
		buf []byte
	)
	// 指明我们能发送的最大长度
	maxSize := http2MaxFrameLen
	// stream级别的流控
	if strQuota := int(l.oiws) - str.bytesOutStanding; strQuota <= 0 {
		str.state = waitingOnStreamQuota
		return false, nil
	} else if maxSize > strQuota {
		maxSize = strQuota
	}
	// 连接级别的流量控制
	if maxSize > int(l.sendQuota) {
		maxSize = int(l.sendQuota)
	}
	// 计算header和data能发送的最长长度
	hSize := min(maxSize, len(dataItem.h))
	dSize := min(maxSize-hSize, len(dataItem.d))
	if hSize != 0 {
		if dSize == 0 {
			buf = dataItem.h
		} else {
			// We can add some data to grpc message header to distribute bytes more equally across frames.
			// Copy on the stack to avoid generating garbage
			var localBuf [http2MaxFrameLen]byte
			copy(localBuf[:hSize], dataItem.h)
			copy(localBuf[hSize:], dataItem.d[:dSize])
			buf = localBuf[:hSize+dSize]
		}
	} else {
		buf = dataItem.d
	}
	size := hSize + dSize
	// Now that outgoing flow controls are checked we can replenish str's write quota
	str.wq.replenish(size)
	var endStream bool
	// If this is the last data message on this stream and all of it can be written in this iteration.
	if dataItem.endStream && len(dataItem.h)+len(dataItem.d) <= size {
		endStream = true
	}
	if dataItem.onEachWrite != nil {
		dataItem.onEachWrite()
	}
	if err := l.framer.fr.WriteData(dataItem.streamID, endStream, buf[:size]); err != nil {
		return false, err
	}
	str.bytesOutStanding += size
	l.sendQuota -= uint32(size)
	dataItem.h = dataItem.h[hSize:]
	dataItem.d = dataItem.d[dSize:]
	if len(dataItem.h) == 0 && len(dataItem.d) == 0 {
		// 若data已经发送完成，则将此data帧移除
		str.itl.dequeue()
	}
	if str.itl.isEmpty() {
		str.state = empty
	} else if trailer, ok := str.itl.peek().(*headerFrame); ok { // The next item is trailers.
		if err := l.writeHeader(trailer.streamID, trailer.endStream, trailer.hf, trailer.onWrite); err != nil {
			return false, err
		}
		if err := l.cleanupStreamHandler(trailer.cleanup); err != nil {
			return false, err
		}
	} else if int(l.oiws)-str.bytesOutStanding <= 0 {
		// 发送配额不够了，进入等待配额状态
		str.state = waitingOnStreamQuota
	} else {
		// 否则加入到activeStream尾部
		l.activeStreams.enqueue(str)
	}
	return false, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
