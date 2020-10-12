package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/grpc/internal/grpcutil"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/internal/grpcrand"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
)

const (
	maxPingStrikes     = 2
	defaultPingTimeout = 2 * time.Hour
)

var (
	// 因为stream状态导致的设置header状态异常
	ErrIllegalHeaderWrite = errors.New("transport: the stream is done or WriteHeader was already called")
	// header list size is larger than the limit set by peer.
	ErrHeaderListSizeLimitViolation = errors.New("transport: trying to send header list size larger than the limit set by peer")
)

var serverConnectionCounter uint64

var goAwayPing = &ping{data: [8]byte{1, 6, 1, 8, 0, 3, 3, 9}}

type http2Server struct {
	lastRead              int64                       // 最后一次读到二进制帧的时间
	ctx                   context.Context             // ctx
	done                  chan struct{}               // 是否已经结束
	conn                  net.Conn                    // 对应的tcp连接
	loopy                 *loopyWriter                // 发送帧循环
	readerDone            chan struct{}               // sync point to enable testing.
	writerDone            chan struct{}               // sync point to enable testing.
	remoteAddr            net.Addr                    // 对端地址
	localAddr             net.Addr                    // 本地地址
	maxStreamID           uint32                      // 最大的stream_id
	authInfo              credentials.AuthInfo        // 鉴权信息
	inTapHandle           tap.ServerInHandle          // 操作header时候会调用
	framer                *framer                     // 二进制帧读取器
	maxStreams            uint32                      // 最大的并发流的个数
	controlBuf            *controlBuffer              // 存储二进制帧的地方(跟loopyWriter配合使用)
	fc                    *trInFlow                   // 流控
	kp                    keepalive.ServerParameters  // 保活参数
	kep                   keepalive.EnforcementPolicy // 保活策略
	lastPingAt            time.Time                   // 最后一次ping的时间
	pingStrikes           uint8                       // 客户端违背保活策略的次数
	resetPingStrikes      uint32                      // 是否需要被重置，当有数据发送的时候设置
	initialWindowSize     int32                       // 初始化窗口大小
	bdpEst                *bdpEstimator               // ？？？？？？
	maxSendHeaderListSize *uint32                     // 最大的头部发送列表长度
	mu                    sync.Mutex                  // guard the following
	// drainChan is initialized when drain(...) is called the first time.
	// After which the server writes out the first GoAway(with ID 2^31-1) frame.
	// Then an independent goroutine will be launched to later send the second GoAway.
	// During this time we don't want to write another first GoAway(with ID 2^31 -1) frame.
	// Thus call to drain(...) will be a no-op if drainChan is already initialized since draining is
	// already underway.
	drainChan     chan struct{}
	state         transportState     // transport状态
	activeStreams map[uint32]*Stream // 活动的stream
	idle          time.Time          // connection空闲时间
	bufferPool    *bufferPool        // bufferPool
	connectionID  uint64             // 连接id
}

// 初始化一个http2服务器
func newHTTP2Server(conn net.Conn, config *ServerConfig) (_ ServerTransport, err error) {
	writeBufSize := config.WriteBufferSize
	readBufSize := config.ReadBufferSize
	maxHeaderListSize := defaultServerMaxHeaderListSize
	if config.MaxHeaderListSize != nil {
		maxHeaderListSize = *config.MaxHeaderListSize
	}
	framer := newFramer(conn, writeBufSize, readBufSize, maxHeaderListSize)
	// 发送setting帧给客户端
	settings := []http2.Setting{{
		ID:  http2.SettingMaxFrameSize,
		Val: http2MaxFrameLen,
	}}
	maxStreams := config.MaxStreams
	if maxStreams == 0 {
		maxStreams = math.MaxUint32
	} else {
		settings = append(settings, http2.Setting{
			ID:  http2.SettingMaxConcurrentStreams,
			Val: maxStreams,
		})
	}
	dynamicWindow := true
	iwz := int32(initialWindowSize)
	if config.InitialWindowSize >= defaultWindowSize {
		iwz = config.InitialWindowSize
		dynamicWindow = false
	}
	icwz := int32(initialWindowSize)
	if config.InitialConnWindowSize >= defaultWindowSize {
		icwz = config.InitialConnWindowSize
		dynamicWindow = false
	}
	if iwz != defaultWindowSize {
		settings = append(settings, http2.Setting{
			ID:  http2.SettingInitialWindowSize,
			Val: uint32(iwz)})
	}
	if config.MaxHeaderListSize != nil {
		settings = append(settings, http2.Setting{
			ID:  http2.SettingMaxHeaderListSize,
			Val: *config.MaxHeaderListSize,
		})
	}
	if config.HeaderTableSize != nil {
		settings = append(settings, http2.Setting{
			ID:  http2.SettingHeaderTableSize,
			Val: *config.HeaderTableSize,
		})
	}
	// 发送setting帧
	if err := framer.fr.WriteSettings(settings...); err != nil {
		return nil, connectionErrorf(false, err, "transport: %v", err)
	}
	// Adjust the connection flow control window if needed.
	if delta := uint32(icwz - defaultWindowSize); delta > 0 {
		if err := framer.fr.WriteWindowUpdate(0, delta); err != nil {
			return nil, connectionErrorf(false, err, "transport: %v", err)
		}
	}
	kp := config.KeepaliveParams
	if kp.MaxConnectionIdle == 0 {
		kp.MaxConnectionIdle = defaultMaxConnectionIdle
	}
	if kp.MaxConnectionAge == 0 {
		kp.MaxConnectionAge = defaultMaxConnectionAge
	}
	kp.MaxConnectionAge += getJitter(kp.MaxConnectionAge)
	if kp.MaxConnectionAgeGrace == 0 {
		kp.MaxConnectionAgeGrace = defaultMaxConnectionAgeGrace
	}
	if kp.Time == 0 {
		kp.Time = defaultServerKeepaliveTime
	}
	if kp.Timeout == 0 {
		kp.Timeout = defaultServerKeepaliveTimeout
	}
	kep := config.KeepalivePolicy
	if kep.MinTime == 0 {
		kep.MinTime = defaultKeepalivePolicyMinTime
	}
	done := make(chan struct{})
	t := &http2Server{
		ctx:               context.Background(),
		done:              done,
		conn:              conn,
		remoteAddr:        conn.RemoteAddr(),
		localAddr:         conn.LocalAddr(),
		authInfo:          config.AuthInfo,
		framer:            framer,
		readerDone:        make(chan struct{}),
		writerDone:        make(chan struct{}),
		maxStreams:        maxStreams,
		inTapHandle:       config.InTapHandle,
		fc:                &trInFlow{limit: uint32(icwz)},
		state:             reachable,
		activeStreams:     make(map[uint32]*Stream),
		kp:                kp,
		idle:              time.Now(),
		kep:               kep,
		initialWindowSize: iwz,
		bufferPool:        newBufferPool(),
	}
	t.controlBuf = newControlBuffer(t.done)
	if dynamicWindow {
		t.bdpEst = &bdpEstimator{
			bdp:               initialWindowSize,
			updateFlowControl: t.updateFlowControl,
		}
	}
	t.connectionID = atomic.AddUint64(&serverConnectionCounter, 1)
	t.framer.writer.Flush()
	defer func() {
		if err != nil {
			t.Close()
		}
	}()
	// Check the validity of client preface.
	preface := make([]byte, len(clientPreface))
	if _, err := io.ReadFull(t.conn, preface); err != nil {
		return nil, connectionErrorf(false, err, "transport: http2Server.HandleStreams failed to receive the preface from client: %v", err)
	}
	if !bytes.Equal(preface, clientPreface) {
		return nil, connectionErrorf(false, nil, "transport: http2Server.HandleStreams received bogus greeting from client: %q", preface)
	}
	frame, err := t.framer.fr.ReadFrame()
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return nil, err
	}
	if err != nil {
		return nil, connectionErrorf(false, err, "transport: http2Server.HandleStreams failed to read initial settings frame: %v", err)
	}
	atomic.StoreInt64(&t.lastRead, time.Now().UnixNano())
	sf, ok := frame.(*http2.SettingsFrame)
	if !ok {
		return nil, connectionErrorf(false, nil, "transport: http2Server.HandleStreams saw invalid preface type %T from client", frame)
	}
	t.handleSettings(sf)
	go func() {
		t.loopy = newLoopyWriter(serverSide, t.framer, t.controlBuf, t.bdpEst)
		t.loopy.ssGoAwayHandler = t.outgoingGoAwayHandler
		if err := t.loopy.run(); err != nil {
			if logger.V(logLevel) {
				logger.Errorf("transport: loopyWriter.run returning. Err: %v", err)
			}
		}
		t.conn.Close()
		close(t.writerDone)
	}()
	go t.keepalive()
	return t, nil
}

// 处理一个stream
func (t *http2Server) HandleStreams(handle func(*Stream), traceCtx func(context.Context, string) context.Context) {
	defer close(t.readerDone)
	for {
		t.controlBuf.throttle()
		// 读取一个二进制帧
		frame, err := t.framer.fr.ReadFrame()
		// 更新最后一次读取frame的时间
		atomic.StoreInt64(&t.lastRead, time.Now().UnixNano())
		if err != nil {
			// 若为stream异常
			// 则关闭对应流
			if se, ok := err.(http2.StreamError); ok {
				if logger.V(logLevel) {
					logger.Warningf("transport: http2Server.HandleStreams encountered http2.StreamError: %v", se)
				}
				t.mu.Lock()
				s := t.activeStreams[se.StreamID]
				t.mu.Unlock()
				if s != nil {
					t.closeStream(s, true, se.Code, false)
				} else {
					t.controlBuf.put(&cleanupStream{
						streamID: se.StreamID,
						rst:      true,
						rstCode:  se.Code,
						onWrite:  func() {},
					})
				}
				continue
			}
			// 若非stream异常，直接关闭服务器
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				t.Close()
				return
			}
			if logger.V(logLevel) {
				logger.Warningf("transport: http2Server.HandleStreams failed to read frame: %v", err)
			}
			t.Close()
			return
		}
		switch frame := frame.(type) {
		case *http2.MetaHeadersFrame:
			if t.operateHeaders(frame, handle, traceCtx) {
				t.Close()
				break
			}
		case *http2.DataFrame:
			t.handleData(frame)
		case *http2.RSTStreamFrame:
			t.handleRSTStream(frame)
		case *http2.SettingsFrame:
			t.handleSettings(frame)
		case *http2.PingFrame:
			t.handlePing(frame)
		case *http2.WindowUpdateFrame:
			t.handleWindowUpdate(frame)
		case *http2.GoAwayFrame:
		default:
			if logger.V(logLevel) {
				logger.Errorf("transport: http2Server.HandleStreams found unhandled frame type %v.", frame)
			}
		}
	}
}

// 操作header二进制帧
func (t *http2Server) operateHeaders(frame *http2.MetaHeadersFrame, handle func(*Stream), traceCtx func(context.Context, string) context.Context) (fatal bool) {
	// 获取stream_id
	streamID := frame.Header().StreamID
	state := &decodeState{
		serverSide: true,
	}
	// 解码头部
	if err := state.decodeHeader(frame); err != nil {
		if se, ok := status.FromError(err); ok {
			t.controlBuf.put(&cleanupStream{
				streamID: streamID,
				rst:      true,
				rstCode:  statusCodeConvTab[se.Code()],
				onWrite:  func() {},
			})
		}
		return false
	}
	buf := newRecvBuffer()
	s := &Stream{
		id:             streamID,
		st:             t,
		buf:            buf,
		fc:             &inFlow{limit: uint32(t.initialWindowSize)},
		recvCompress:   state.data.encoding,
		method:         state.data.method,
		contentSubtype: state.data.contentSubtype,
	}
	if frame.StreamEnded() {
		s.state = streamReadDone
	}
	// 是否设置超时时间
	if state.data.timeoutSet {
		s.ctx, s.cancel = context.WithTimeout(t.ctx, state.data.timeout)
	} else {
		s.ctx, s.cancel = context.WithCancel(t.ctx)
	}
	pr := &peer.Peer{
		Addr: t.remoteAddr,
	}
	if t.authInfo != nil {
		pr.AuthInfo = t.authInfo
	}
	s.ctx = peer.NewContext(s.ctx, pr)
	if len(state.data.mdata) > 0 {
		s.ctx = metadata.NewIncomingContext(s.ctx, state.data.mdata)
	}
	if t.inTapHandle != nil {
		var err error
		info := &tap.Info{
			FullMethodName: state.data.method,
		}
		s.ctx, err = t.inTapHandle(s.ctx, info)
		if err != nil {
			if logger.V(logLevel) {
				logger.Warningf("transport: http2Server.operateHeaders got an error from InTapHandle: %v", err)
			}
			t.controlBuf.put(&cleanupStream{
				streamID: s.id,
				rst:      true,
				rstCode:  http2.ErrCodeRefusedStream,
				onWrite:  func() {},
			})
			s.cancel()
			return false
		}
	}
	t.mu.Lock()
	if t.state != reachable {
		t.mu.Unlock()
		s.cancel()
		return false
	}
	if uint32(len(t.activeStreams)) >= t.maxStreams {
		t.mu.Unlock()
		t.controlBuf.put(&cleanupStream{
			streamID: streamID,
			rst:      true,
			rstCode:  http2.ErrCodeRefusedStream,
			onWrite:  func() {},
		})
		s.cancel()
		return false
	}
	if streamID%2 != 1 || streamID <= t.maxStreamID {
		t.mu.Unlock()
		if logger.V(logLevel) {
			logger.Errorf("transport: http2Server.HandleStreams received an illegal stream id: %v", streamID)
		}
		s.cancel()
		return true
	}
	t.maxStreamID = streamID
	t.activeStreams[streamID] = s
	if len(t.activeStreams) == 1 {
		t.idle = time.Time{}
	}
	t.mu.Unlock()
	s.requestRead = func(n int) {
		t.adjustWindow(s, uint32(n))
	}
	s.ctx = traceCtx(s.ctx, s.method)
	s.ctxDone = s.ctx.Done()
	s.wq = newWriteQuota(defaultWriteQuota, s.ctxDone)
	s.trReader = &transportReader{
		reader: &recvBufferReader{
			ctx:        s.ctx,
			ctxDone:    s.ctxDone,
			recv:       s.buf,
			freeBuffer: t.bufferPool.put,
		},
		windowHandler: func(n int) {
			t.updateWindow(s, uint32(n))
		},
	}
	t.controlBuf.put(&registerStream{
		streamID: s.id,
		wq:       s.wq,
	})
	handle(s)
	return false
}

// 告知发送端接受配额可以更新
// 主要针对data数据帧，用作流控(连接或者stream级别均可)
func (t *http2Server) updateWindow(s *Stream, n uint32) {
	if w := s.fc.onRead(n); w > 0 {
		t.controlBuf.put(&outgoingWindowUpdate{streamID: s.id,
			increment: w,
		})
	}
}

// 依据bdp决定是否需要更新输入流流控窗口
func (t *http2Server) updateFlowControl(n uint32) {
	t.mu.Lock()
	for _, s := range t.activeStreams {
		s.fc.newLimit(n)
	}
	t.initialWindowSize = int32(n)
	t.mu.Unlock()
	t.controlBuf.put(&outgoingWindowUpdate{
		streamID:  0,
		increment: t.fc.newLimit(n),
	})
	t.controlBuf.put(&outgoingSettings{
		ss: []http2.Setting{
			{
				ID:  http2.SettingInitialWindowSize,
				Val: n,
			},
		},
	})
}

// 数据帧处理函数
func (t *http2Server) handleData(f *http2.DataFrame) {
	size := f.Header().Length
	var sendBDPPing bool
	if t.bdpEst != nil {
		sendBDPPing = t.bdpEst.add(size)
	}
	// 若可以处理此data，则发送窗口更新帧
	if w := t.fc.onData(size); w > 0 {
		t.controlBuf.put(&outgoingWindowUpdate{
			streamID:  0,
			increment: w,
		})
	}
	if sendBDPPing {
		// Avoid excessive ping detection (e.g. in an L7 proxy)
		// by sending a window update prior to the BDP ping.
		if w := t.fc.reset(); w > 0 {
			t.controlBuf.put(&outgoingWindowUpdate{
				streamID:  0,
				increment: w,
			})
		}
		t.controlBuf.put(bdpPing)
	}
	s, ok := t.getStream(f)
	if !ok {
		return
	}
	if size > 0 {
		if err := s.fc.onData(size); err != nil {
			t.closeStream(s, true, http2.ErrCodeFlowControl, false)
			return
		}
		if f.Header().Flags.Has(http2.FlagDataPadded) {
			if w := s.fc.onRead(size - uint32(len(f.Data()))); w > 0 {
				t.controlBuf.put(&outgoingWindowUpdate{s.id, w})
			}
		}
		if len(f.Data()) > 0 {
			buffer := t.bufferPool.get()
			buffer.Reset()
			buffer.Write(f.Data())
			s.write(recvMsg{buffer: buffer})
		}
	}
	if f.Header().Flags.Has(http2.FlagDataEndStream) {
		s.compareAndSwapState(streamActive, streamReadDone)
		s.write(recvMsg{err: io.EOF})
	}
}

func (t *http2Server) handleRSTStream(f *http2.RSTStreamFrame) {
	// If the stream is not deleted from the transport's active streams map, then do a regular close stream.
	if s, ok := t.getStream(f); ok {
		t.closeStream(s, false, 0, false)
		return
	}
	// If the stream is already deleted from the active streams map, then put a cleanupStream item into controlbuf to delete the stream from loopy writer's established streams map.
	t.controlBuf.put(&cleanupStream{
		streamID: f.Header().StreamID,
		rst:      false,
		rstCode:  0,
		onWrite:  func() {},
	})
}

func (t *http2Server) handleSettings(f *http2.SettingsFrame) {
	if f.IsAck() {
		return
	}
	var ss []http2.Setting
	var updateFuncs []func()
	f.ForeachSetting(func(s http2.Setting) error {
		switch s.ID {
		case http2.SettingMaxHeaderListSize:
			updateFuncs = append(updateFuncs, func() {
				t.maxSendHeaderListSize = new(uint32)
				*t.maxSendHeaderListSize = s.Val
			})
		default:
			ss = append(ss, s)
		}
		return nil
	})
	t.controlBuf.executeAndPut(func(interface{}) bool {
		for _, f := range updateFuncs {
			f()
		}
		return true
	}, &incomingSettings{
		ss: ss,
	})
}

func (t *http2Server) handlePing(f *http2.PingFrame) {
	// 如果是ack，则代表是客户端响应服务器的ack报文
	if f.IsAck() {
		if f.Data == goAwayPing.data && t.drainChan != nil {
			close(t.drainChan)
			return
		}
		// 有可能是BDP的ping报文
		if t.bdpEst != nil {
			t.bdpEst.calculate(f.Data)
		}
		return
	}
	pingAck := &ping{ack: true}
	copy(pingAck.data[:], f.Data[:])
	t.controlBuf.put(pingAck)
	now := time.Now()
	defer func() {
		t.lastPingAt = now
	}()
	// A reset ping strikes means that we don't need to check for policy
	// violation for this ping and the pingStrikes counter should be set
	// to 0.
	if atomic.CompareAndSwapUint32(&t.resetPingStrikes, 1, 0) {
		t.pingStrikes = 0
		return
	}
	t.mu.Lock()
	ns := len(t.activeStreams)
	t.mu.Unlock()
	if ns < 1 && !t.kep.PermitWithoutStream {
		// Keepalive shouldn't be active thus, this new ping should
		// have come after at least defaultPingTimeout.
		if t.lastPingAt.Add(defaultPingTimeout).After(now) {
			t.pingStrikes++
		}
	} else {
		// Check if keepalive policy is respected.
		if t.lastPingAt.Add(t.kep.MinTime).After(now) {
			t.pingStrikes++
		}
	}

	if t.pingStrikes > maxPingStrikes {
		// Send goaway and close the connection.
		if logger.V(logLevel) {
			logger.Errorf("transport: Got too many pings from the client, closing the connection.")
		}
		t.controlBuf.put(&goAway{code: http2.ErrCodeEnhanceYourCalm, debugData: []byte("too_many_pings"), closeConn: true})
	}
}

func (t *http2Server) handleWindowUpdate(f *http2.WindowUpdateFrame) {
	t.controlBuf.put(&incomingWindowUpdate{
		streamID:  f.Header().StreamID,
		increment: f.Increment,
	})
}

func appendHeaderFieldsFromMD(headerFields []hpack.HeaderField, md metadata.MD) []hpack.HeaderField {
	for k, vv := range md {
		if isReservedHeader(k) {
			// Clients don't tolerate reading restricted headers after some non restricted ones were sent.
			continue
		}
		for _, v := range vv {
			headerFields = append(headerFields, hpack.HeaderField{Name: k, Value: encodeMetadataHeader(k, v)})
		}
	}
	return headerFields
}

func (t *http2Server) checkForHeaderListSize(it interface{}) bool {
	if t.maxSendHeaderListSize == nil {
		return true
	}
	hdrFrame := it.(*headerFrame)
	var sz int64
	for _, f := range hdrFrame.hf {
		if sz += int64(f.Size()); sz > int64(*t.maxSendHeaderListSize) {
			if logger.V(logLevel) {
				logger.Errorf("header list size to send violates the maximum size (%d bytes) set by client", *t.maxSendHeaderListSize)
			}
			return false
		}
	}
	return true
}

func (t *http2Server) WriteHeader(s *Stream, md metadata.MD) error {
	if s.updateHeaderSent() || s.getState() == streamDone {
		return ErrIllegalHeaderWrite
	}
	s.hdrMu.Lock()
	if md.Len() > 0 {
		if s.header.Len() > 0 {
			s.header = metadata.Join(s.header, md)
		} else {
			s.header = md
		}
	}
	if err := t.writeHeaderLocked(s); err != nil {
		s.hdrMu.Unlock()
		return err
	}
	s.hdrMu.Unlock()
	return nil
}

func (t *http2Server) setResetPingStrikes() {
	atomic.StoreUint32(&t.resetPingStrikes, 1)
}

func (t *http2Server) writeHeaderLocked(s *Stream) error {
	// TODO(mmukhi): Benchmark if the performance gets better if count the metadata and other header fields
	// first and create a slice of that exact size.
	headerFields := make([]hpack.HeaderField, 0, 2) // at least :status, content-type will be there if none else.
	headerFields = append(headerFields, hpack.HeaderField{Name: ":status", Value: "200"})
	headerFields = append(headerFields, hpack.HeaderField{Name: "content-type", Value: grpcutil.ContentType(s.contentSubtype)})
	if s.sendCompress != "" {
		headerFields = append(headerFields, hpack.HeaderField{Name: "grpc-encoding", Value: s.sendCompress})
	}
	headerFields = appendHeaderFieldsFromMD(headerFields, s.header)
	success, err := t.controlBuf.executeAndPut(t.checkForHeaderListSize, &headerFrame{
		streamID:  s.id,
		hf:        headerFields,
		endStream: false,
		onWrite:   t.setResetPingStrikes,
	})
	if !success {
		if err != nil {
			return err
		}
		t.closeStream(s, true, http2.ErrCodeInternal, false)
		return ErrHeaderListSizeLimitViolation
	}
	return nil
}

func (t *http2Server) WriteStatus(s *Stream, st *status.Status) error {
	if s.getState() == streamDone {
		return nil
	}
	s.hdrMu.Lock()
	// TODO(mmukhi): Benchmark if the performance gets better if count the metadata and other header fields
	// first and create a slice of that exact size.
	headerFields := make([]hpack.HeaderField, 0, 2) // grpc-status and grpc-message will be there if none else.
	if !s.updateHeaderSent() {                      // No headers have been sent.
		if len(s.header) > 0 { // Send a separate header frame.
			if err := t.writeHeaderLocked(s); err != nil {
				s.hdrMu.Unlock()
				return err
			}
		} else { // Send a trailer only response.
			headerFields = append(headerFields, hpack.HeaderField{Name: ":status", Value: "200"})
			headerFields = append(headerFields, hpack.HeaderField{Name: "content-type", Value: grpcutil.ContentType(s.contentSubtype)})
		}
	}
	headerFields = append(headerFields, hpack.HeaderField{Name: "grpc-status", Value: strconv.Itoa(int(st.Code()))})
	headerFields = append(headerFields, hpack.HeaderField{Name: "grpc-message", Value: encodeGrpcMessage(st.Message())})

	if p := st.Proto(); p != nil && len(p.Details) > 0 {
		stBytes, err := proto.Marshal(p)
		if err != nil {
			// TODO: return error instead, when callers are able to handle it.
			logger.Errorf("transport: failed to marshal rpc status: %v, error: %v", p, err)
		} else {
			headerFields = append(headerFields, hpack.HeaderField{Name: "grpc-status-details-bin", Value: encodeBinHeader(stBytes)})
		}
	}

	// Attach the trailer metadata.
	headerFields = appendHeaderFieldsFromMD(headerFields, s.trailer)
	trailingHeader := &headerFrame{
		streamID:  s.id,
		hf:        headerFields,
		endStream: true,
		onWrite:   t.setResetPingStrikes,
	}
	s.hdrMu.Unlock()
	success, err := t.controlBuf.execute(t.checkForHeaderListSize, trailingHeader)
	if !success {
		if err != nil {
			return err
		}
		t.closeStream(s, true, http2.ErrCodeInternal, false)
		return ErrHeaderListSizeLimitViolation
	}
	// Send a RST_STREAM after the trailers if the client has not already half-closed.
	rst := s.getState() == streamActive
	t.finishStream(s, rst, http2.ErrCodeNo, trailingHeader, true)
	return nil
}

// 将data转换为二进制frame
// 并写入controlBuffer，由loopyWriter发送给客户端
func (t *http2Server) Write(s *Stream, hdr []byte, data []byte, opts *Options) error {
	if !s.isHeaderSent() { // Headers haven't been written yet.
		if err := t.WriteHeader(s, nil); err != nil {
			if _, ok := err.(ConnectionError); ok {
				return err
			}
			// TODO(mmukhi, dfawley): Make sure this is the right code to return.
			return status.Errorf(codes.Internal, "transport: %v", err)
		}
	} else {
		// Writing headers checks for this condition.
		if s.getState() == streamDone {
			// TODO(mmukhi, dfawley): Should the server write also return io.EOF?
			s.cancel()
			select {
			case <-t.done:
				return ErrConnClosing
			default:
			}
			return ContextErr(s.ctx.Err())
		}
	}
	df := &dataFrame{
		streamID:    s.id,
		h:           hdr,
		d:           data,
		onEachWrite: t.setResetPingStrikes,
	}
	if err := s.wq.get(int32(len(hdr) + len(data))); err != nil {
		select {
		case <-t.done:
			return ErrConnClosing
		default:
		}
		return ContextErr(s.ctx.Err())
	}
	return t.controlBuf.put(df)
}

// keepalive running in a separate goroutine does the following:
// 1. Gracefully closes an idle connection after a duration of keepalive.MaxConnectionIdle.
// 2. Gracefully closes any connection after a duration of keepalive.MaxConnectionAge.
// 3. Forcibly closes a connection after an additive period of keepalive.MaxConnectionAgeGrace over keepalive.MaxConnectionAge.
// 4. Makes sure a connection is alive by sending pings with a frequency of keepalive.Time and closes a non-responsive connection
// after an additional duration of keepalive.Timeout.
func (t *http2Server) keepalive() {
	p := &ping{}
	// True iff a ping has been sent, and no data has been received since then.
	outstandingPing := false
	// Amount of time remaining before which we should receive an ACK for the
	// last sent ping.
	kpTimeoutLeft := time.Duration(0)
	// Records the last value of t.lastRead before we go block on the timer.
	// This is required to check for read activity since then.
	prevNano := time.Now().UnixNano()
	// Initialize the different timers to their default values.
	idleTimer := time.NewTimer(t.kp.MaxConnectionIdle)
	ageTimer := time.NewTimer(t.kp.MaxConnectionAge)
	kpTimer := time.NewTimer(t.kp.Time)
	defer func() {
		// We need to drain the underlying channel in these timers after a call
		// to Stop(), only if we are interested in resetting them. Clearly we
		// are not interested in resetting them here.
		idleTimer.Stop()
		ageTimer.Stop()
		kpTimer.Stop()
	}()

	for {
		select {
		case <-idleTimer.C:
			t.mu.Lock()
			idle := t.idle
			if idle.IsZero() { // The connection is non-idle.
				t.mu.Unlock()
				idleTimer.Reset(t.kp.MaxConnectionIdle)
				continue
			}
			val := t.kp.MaxConnectionIdle - time.Since(idle)
			t.mu.Unlock()
			if val <= 0 {
				// The connection has been idle for a duration of keepalive.MaxConnectionIdle or more.
				// Gracefully close the connection.
				t.drain(http2.ErrCodeNo, []byte{})
				return
			}
			idleTimer.Reset(val)
		case <-ageTimer.C:
			t.drain(http2.ErrCodeNo, []byte{})
			ageTimer.Reset(t.kp.MaxConnectionAgeGrace)
			select {
			case <-ageTimer.C:
				// Close the connection after grace period.
				if logger.V(logLevel) {
					logger.Infof("transport: closing server transport due to maximum connection age.")
				}
				t.Close()
			case <-t.done:
			}
			return
		case <-kpTimer.C:
			lastRead := atomic.LoadInt64(&t.lastRead)
			if lastRead > prevNano {
				// There has been read activity since the last time we were
				// here. Setup the timer to fire at kp.Time seconds from
				// lastRead time and continue.
				outstandingPing = false
				kpTimer.Reset(time.Duration(lastRead) + t.kp.Time - time.Duration(time.Now().UnixNano()))
				prevNano = lastRead
				continue
			}
			if outstandingPing && kpTimeoutLeft <= 0 {
				if logger.V(logLevel) {
					logger.Infof("transport: closing server transport due to idleness.")
				}
				t.Close()
				return
			}
			if !outstandingPing {
				t.controlBuf.put(p)
				kpTimeoutLeft = t.kp.Timeout
				outstandingPing = true
			}
			// The amount of time to sleep here is the minimum of kp.Time and
			// timeoutLeft. This will ensure that we wait only for kp.Time
			// before sending out the next ping (for cases where the ping is
			// acked).
			sleepDuration := minTime(t.kp.Time, kpTimeoutLeft)
			kpTimeoutLeft -= sleepDuration
			kpTimer.Reset(sleepDuration)
		case <-t.done:
			return
		}
	}
}

func (t *http2Server) Close() error {
	t.mu.Lock()
	if t.state == closing {
		t.mu.Unlock()
		return errors.New("transport: Close() was already called")
	}
	t.state = closing
	streams := t.activeStreams
	t.activeStreams = nil
	t.mu.Unlock()
	t.controlBuf.finish()
	close(t.done)
	err := t.conn.Close()
	for _, s := range streams {
		s.cancel()
	}
	return err
}

// deleteStream deletes the stream s from transport's active streams.
func (t *http2Server) deleteStream(s *Stream, eosReceived bool) {
	// In case stream sending and receiving are invoked in separate
	// goroutines (e.g., bi-directional streaming), cancel needs to be
	// called to interrupt the potential blocking on other goroutines.
	s.cancel()

	t.mu.Lock()
	if _, ok := t.activeStreams[s.id]; ok {
		delete(t.activeStreams, s.id)
		if len(t.activeStreams) == 0 {
			t.idle = time.Now()
		}
	}
	t.mu.Unlock()
}

// finishStream closes the stream and puts the trailing headerFrame into controlbuf.
func (t *http2Server) finishStream(s *Stream, rst bool, rstCode http2.ErrCode, hdr *headerFrame, eosReceived bool) {
	oldState := s.swapState(streamDone)
	if oldState == streamDone {
		// If the stream was already done, return.
		return
	}

	hdr.cleanup = &cleanupStream{
		streamID: s.id,
		rst:      rst,
		rstCode:  rstCode,
		onWrite: func() {
			t.deleteStream(s, eosReceived)
		},
	}
	t.controlBuf.put(hdr)
}

// closeStream clears the footprint of a stream when the stream is not needed any more.
func (t *http2Server) closeStream(s *Stream, rst bool, rstCode http2.ErrCode, eosReceived bool) {
	s.swapState(streamDone)
	t.deleteStream(s, eosReceived)

	t.controlBuf.put(&cleanupStream{
		streamID: s.id,
		rst:      rst,
		rstCode:  rstCode,
		onWrite:  func() {},
	})
}

func (t *http2Server) RemoteAddr() net.Addr {
	return t.remoteAddr
}

func (t *http2Server) Drain() {
	t.drain(http2.ErrCodeNo, []byte{})
}

func (t *http2Server) drain(code http2.ErrCode, debugData []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.drainChan != nil {
		return
	}
	t.drainChan = make(chan struct{})
	t.controlBuf.put(&goAway{code: code, debugData: debugData, headsUp: true})
}

func (t *http2Server) outgoingGoAwayHandler(g *goAway) (bool, error) {
	t.mu.Lock()
	if t.state == closing {
		t.mu.Unlock()
		return false, ErrConnClosing
	}
	sid := t.maxStreamID
	if !g.headsUp {
		// 停止接受更多的流
		t.state = draining
		if len(t.activeStreams) == 0 {
			g.closeConn = true
		}
		t.mu.Unlock()
		if err := t.framer.fr.WriteGoAway(sid, g.code, g.debugData); err != nil {
			return false, err
		}
		if g.closeConn {
			// Abruptly close the connection following the GoAway (via
			// loopywriter).  But flush out what's inside the buffer first.
			t.framer.writer.Flush()
			return false, fmt.Errorf("transport: Connection closing")
		}
		return true, nil
	}
	t.mu.Unlock()
	// For a graceful close, send out a GoAway with stream ID of MaxUInt32,
	// Follow that with a ping and wait for the ack to come back or a timer
	// to expire. During this time accept new streams since they might have
	// originated before the GoAway reaches the client.
	// After getting the ack or timer expiration send out another GoAway this
	// time with an ID of the max stream server intends to process.
	if err := t.framer.fr.WriteGoAway(math.MaxUint32, http2.ErrCodeNo, []byte{}); err != nil {
		return false, err
	}
	if err := t.framer.fr.WritePing(false, goAwayPing.data); err != nil {
		return false, err
	}
	go func() {
		timer := time.NewTimer(time.Minute)
		defer timer.Stop()
		select {
		case <-t.drainChan:
		case <-timer.C:
		case <-t.done:
			return
		}
		t.controlBuf.put(&goAway{code: g.code, debugData: g.debugData})
	}()
	return false, nil
}

// 获取此时的发送配额
func (t *http2Server) getOutFlowWindow() int64 {
	resp := make(chan uint32, 1)
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	t.controlBuf.put(&outFlowControlSizeRequest{resp})
	select {
	case sz := <-resp:
		return int64(sz)
	case <-t.done:
		return -1
	case <-timer.C:
		return -2
	}
}

func (t *http2Server) getStream(f http2.Frame) (*Stream, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.activeStreams == nil {
		return nil, false
	}
	s, ok := t.activeStreams[f.Header().StreamID]
	if !ok {
		return nil, false
	}
	return s, true
}

// adjustWindow sends out extra window update over the initial window size
// of stream if the application is requesting data larger in size than
// the window.
func (t *http2Server) adjustWindow(s *Stream, n uint32) {
	if w := s.fc.maybeAdjust(n); w > 0 {
		t.controlBuf.put(&outgoingWindowUpdate{streamID: s.id, increment: w})
	}
}

func getJitter(v time.Duration) time.Duration {
	if v == infinity {
		return 0
	}
	// Generate a jitter between +/- 10% of the value.
	r := int64(v / 10)
	j := grpcrand.Int63n(2*r) - r
	return time.Duration(j)
}
