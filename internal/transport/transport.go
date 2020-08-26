package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/tap"
)

const logLevel = 2

type streamState uint32

const (
	streamActive    streamState = iota
	streamWriteDone             // 停止写
	streamReadDone              // 停止读
	streamDone                  // 流结束
)

type transportState int

const (
	reachable transportState = iota
	closing
	draining
)

type bufferPool struct {
	pool sync.Pool
}

func newBufferPool() *bufferPool {
	return &bufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

func (p *bufferPool) get() *bytes.Buffer {
	return p.pool.Get().(*bytes.Buffer)
}

func (p *bufferPool) put(b *bytes.Buffer) {
	p.pool.Put(b)
}

// 表示收到的一个msg从transport，并且所有传输协议既定的信息已经删除
type recvMsg struct {
	buffer *bytes.Buffer
	// nil: 收到了msg
	// io.EOF: 流已经完成，data是nil
	// other: 传输失败，data是nil
	err error
}

// recv msg buffer
type recvBuffer struct {
	c       chan recvMsg
	mu      sync.Mutex
	backlog []recvMsg // 积压的未读消息
	err     error
}

func newRecvBuffer() *recvBuffer {
	b := &recvBuffer{
		c: make(chan recvMsg, 1),
	}
	return b
}

// 放入一个msg到buffer
func (b *recvBuffer) put(r recvMsg) {
	b.mu.Lock()
	if b.err != nil {
		b.mu.Unlock()
		// 错误优先发生，停止接受更多的流
		return
	}
	b.err = r.err
	// 若backlog为0，则代表chan至多有一个msg，此时向chan写入数据大概率成功
	// 后续避免再从切片拷贝
	if len(b.backlog) == 0 {
		select {
		case b.c <- r:
			b.mu.Unlock()
			return
		default:
		}
	}
	b.backlog = append(b.backlog, r)
	b.mu.Unlock()
}

// 加载一个msg到chan
func (b *recvBuffer) load() {
	b.mu.Lock()
	if len(b.backlog) > 0 {
		select {
		case b.c <- b.backlog[0]:
			b.backlog[0] = recvMsg{}
			b.backlog = b.backlog[1:]
		default:
		}
	}
	b.mu.Unlock()
}

func (b *recvBuffer) get() <-chan recvMsg {
	return b.c
}

type recvBufferReader struct {
	closeStream func(error)     // 用给定的err和nil trailer metadata关闭client transport stream
	ctx         context.Context // ctx
	ctxDone     <-chan struct{} // cache of ctx.Done() (for performance).
	recv        *recvBuffer     // recvBuffer
	last        *bytes.Buffer   // 缓存之前调用留下的数据
	err         error
	freeBuffer  func(*bytes.Buffer)
}

// 从recv buffer里面读取给定切片长度的数据
func (r *recvBufferReader) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.last != nil {
		copied, _ := r.last.Read(p)
		if r.last.Len() == 0 {
			r.freeBuffer(r.last)
			r.last = nil
		}
		return copied, nil
	}
	if r.closeStream != nil {
		n, r.err = r.readClient(p)
	} else {
		n, r.err = r.read(p)
	}
	return n, r.err
}

func (r *recvBufferReader) read(p []byte) (n int, err error) {
	select {
	case <-r.ctxDone:
		return 0, ContextErr(r.ctx.Err())
	case m := <-r.recv.get():
		return r.readAdditional(m, p)
	}
}

func (r *recvBufferReader) readClient(p []byte) (n int, err error) {
	// If the context is canceled, then closes the stream with nil metadata.
	// closeStream writes its error parameter to r.recv as a recvMsg.
	// r.readAdditional acts on that message and returns the necessary error.
	select {
	case <-r.ctxDone:
		// Note that this adds the ctx error to the end of recv buffer, and
		// reads from the head. This will delay the error until recv buffer is
		// empty, thus will delay ctx cancellation in Recv().
		//
		// It's done this way to fix a race between ctx cancel and trailer. The
		// race was, stream.Recv() may return ctx error if ctxDone wins the
		// race, but stream.Trailer() may return a non-nil md because the stream
		// was not marked as done when trailer is received. This closeStream
		// call will mark stream as done, thus fix the race.
		//
		// TODO: delaying ctx error seems like a unnecessary side effect. What
		// we really want is to mark the stream as done, and return ctx error
		// faster.
		r.closeStream(ContextErr(r.ctx.Err()))
		m := <-r.recv.get()
		return r.readAdditional(m, p)
	case m := <-r.recv.get():
		return r.readAdditional(m, p)
	}
}

func (r *recvBufferReader) readAdditional(m recvMsg, p []byte) (n int, err error) {
	r.recv.load()
	if m.err != nil {
		return 0, m.err
	}
	copied, _ := m.buffer.Read(p)
	if m.buffer.Len() == 0 {
		r.freeBuffer(m.buffer)
		r.last = nil
	} else {
		r.last = m.buffer
	}
	return copied, nil
}

// 代表了一次rpc访问
type Stream struct {
	id               uint32             // 流的标识符
	st               ServerTransport    // 对于client来说为nil
	ct               *http2Client       // 对于server来说为nil
	ctx              context.Context    // 流关联的ctx
	cancel           context.CancelFunc // 对于客户端来说为nil
	done             chan struct{}      // 客户端生效
	ctxDone          <-chan struct{}    // 服务端生效
	method           string             // rpc方法
	recvCompress     string             // 输入流解压缩器
	sendCompress     string             // 输出流解压缩器
	buf              *recvBuffer        // 消息读取器
	trReader         io.Reader          // 用以封装recvBuffer
	fc               *inFlow            // 输入流流控
	wq               *writeQuota        // 输出流流控
	requestRead      func(int)          // Callback to state application's intentions to read data. This is used to adjust flow control, if needed.
	headerChan       chan struct{}      // closed to indicate the end of header metadata.
	headerChanClosed uint32             // 防止headerChan关闭多次
	headerValid      bool               // 客户端有效
	hdrMu            sync.Mutex         // 服务端有效
	header           metadata.MD        // 对于客户端表示客户端接收到的header metadata；对于服务端，保存调用SetHeader()的header，header会被合并当调用WriteHeader()
	trailer          metadata.MD        // trailer metadata.
	noHeaders        bool               // 如果客户端没有收到header，设置此参数(只有当stream done才会设置).
	headerSent       uint32             // 在服务端，当头被发送之后会设置
	state            streamState        // 流状态
	status           *status.Status     // On client-side it is the status error received from the server. On server-side it is unused.
	bytesReceived    uint32             // indicates whether any bytes have been received on this stream
	unprocessed      uint32             // set if the server sends a refused stream or GOAWAY including this stream
	contentSubtype   string             // contentSubtype is the content-subtype for requests. this must be lowercase or the behavior is undefined.
}

// 只在服务端有效
func (s *Stream) isHeaderSent() bool {
	return atomic.LoadUint32(&s.headerSent) == 1
}

// 设置header已经被发送
func (s *Stream) updateHeaderSent() bool {
	return atomic.SwapUint32(&s.headerSent, 1) == 1
}

// stream状态相关
func (s *Stream) swapState(st streamState) streamState {
	return streamState(atomic.SwapUint32((*uint32)(&s.state), uint32(st)))
}

func (s *Stream) compareAndSwapState(oldState, newState streamState) bool {
	return atomic.CompareAndSwapUint32((*uint32)(&s.state), uint32(oldState), uint32(newState))
}

func (s *Stream) getState() streamState {
	return streamState(atomic.LoadUint32((*uint32)(&s.state)))
}

func (s *Stream) waitOnHeader() {
	if s.headerChan == nil {
		// On the server headerChan is always nil since a stream originates
		// only after having received headers.
		return
	}
	select {
	case <-s.ctx.Done():
		// Close the stream to prevent headers/trailers from changing after
		// this function returns.
		s.ct.CloseStream(s, ContextErr(s.ctx.Err()))
		// headerChan could possibly not be closed yet if closeStream raced
		// with operateHeaders; wait until it is closed explicitly here.
		<-s.headerChan
	case <-s.headerChan:
	}
}

func (s *Stream) RecvCompress() string {
	s.waitOnHeader()
	return s.recvCompress
}

func (s *Stream) SetSendCompress(str string) {
	s.sendCompress = str
}

// 当客户端收到服务端发送来的最终的status，此chan会被关闭
func (s *Stream) Done() <-chan struct{} {
	return s.done
}

// Header returns the header metadata of the stream.
//
// On client side, it acquires the key-value pairs of header metadata once it is
// available. It blocks until i) the metadata is ready or ii) there is no header
// metadata or iii) the stream is canceled/expired.
//
// On server side, it returns the out header after t.WriteHeader is called.  It
// does not block and must not be called until after WriteHeader.
func (s *Stream) Header() (metadata.MD, error) {
	if s.headerChan == nil {
		// On server side, return the header in stream. It will be the out
		// header after t.WriteHeader is called.
		return s.header.Copy(), nil
	}
	s.waitOnHeader()
	if !s.headerValid {
		return nil, s.status.Err()
	}
	return s.header.Copy(), nil
}

// TrailersOnly blocks until a header or trailers-only frame is received and
// then returns true if the stream was trailers-only.  If the stream ends
// before headers are received, returns true, nil.  Client-side only.
func (s *Stream) TrailersOnly() bool {
	s.waitOnHeader()
	return s.noHeaders
}

// Trailer returns the cached trailer metedata. Note that if it is not called
// after the entire stream is done, it could return an empty MD. Client
// side only.
// It can be safely read only after stream has ended that is either read
// or write have returned io.EOF.
func (s *Stream) Trailer() metadata.MD {
	c := s.trailer.Copy()
	return c
}

// Example：application/grpc+proto
func (s *Stream) ContentSubtype() string {
	return s.contentSubtype
}

func (s *Stream) Context() context.Context {
	return s.ctx
}

func (s *Stream) Method() string {
	return s.method
}

// 只有当stream为dong也就是Done() chan关闭之后才会被读取
func (s *Stream) Status() *status.Status {
	return s.status
}

// 服务端设置header
func (s *Stream) SetHeader(md metadata.MD) error {
	if md.Len() == 0 {
		return nil
	}
	if s.isHeaderSent() || s.getState() == streamDone {
		return ErrIllegalHeaderWrite
	}
	s.hdrMu.Lock()
	s.header = metadata.Join(s.header, md)
	s.hdrMu.Unlock()
	return nil
}

func (s *Stream) SendHeader(md metadata.MD) error {
	return s.st.WriteHeader(s, md)
}

// SetTrailer sets the trailer metadata which will be sent with the RPC status
// by the server. This can be called multiple times. Server side only.
// This should not be called parallel to other data writes.
func (s *Stream) SetTrailer(md metadata.MD) error {
	if md.Len() == 0 {
		return nil
	}
	if s.getState() == streamDone {
		return ErrIllegalHeaderWrite
	}
	s.hdrMu.Lock()
	s.trailer = metadata.Join(s.trailer, md)
	s.hdrMu.Unlock()
	return nil
}

func (s *Stream) Read(p []byte) (n int, err error) {
	if er := s.trReader.(*transportReader).er; er != nil {
		return 0, er
	}
	s.requestRead(len(p))
	return io.ReadFull(s.trReader, p)
}

func (s *Stream) BytesReceived() bool {
	return atomic.LoadUint32(&s.bytesReceived) == 1
}

// Unprocessed indicates whether the server did not process this stream --
// i.e. it sent a refused stream or GOAWAY including this stream ID.
func (s *Stream) Unprocessed() bool {
	return atomic.LoadUint32(&s.unprocessed) == 1
}

func (s *Stream) GoString() string {
	return fmt.Sprintf("<stream: %p, %v>", s, s.method)
}

func (s *Stream) write(m recvMsg) {
	s.buf.put(m)
}

type transportReader struct {
	reader        io.Reader
	windowHandler func(int)
	er            error
}

func (t *transportReader) Read(p []byte) (n int, err error) {
	n, err = t.reader.Read(p)
	if err != nil {
		t.er = err
		return
	}
	t.windowHandler(n)
	return
}

type ServerConfig struct {
	MaxStreams            uint32
	AuthInfo              credentials.AuthInfo
	InTapHandle           tap.ServerInHandle
	KeepaliveParams       keepalive.ServerParameters
	KeepalivePolicy       keepalive.EnforcementPolicy
	InitialWindowSize     int32
	InitialConnWindowSize int32
	WriteBufferSize       int
	ReadBufferSize        int
	MaxHeaderListSize     *uint32
	HeaderTableSize       *uint32
}

func NewServerTransport(protocol string, conn net.Conn, config *ServerConfig) (ServerTransport, error) {
	return newHTTP2Server(conn, config)
}

type ConnectOptions struct {
	// UserAgent is the application user agent.
	UserAgent string
	// Dialer specifies how to dial a network address.
	Dialer func(context.Context, string) (net.Conn, error)
	// FailOnNonTempDialError specifies if gRPC fails on non-temporary dial errors.
	FailOnNonTempDialError bool
	// PerRPCCredentials stores the PerRPCCredentials required to issue RPCs.
	PerRPCCredentials []credentials.PerRPCCredentials
	// TransportCredentials stores the Authenticator required to setup a client
	// connection. Only one of TransportCredentials and CredsBundle is non-nil.
	TransportCredentials credentials.TransportCredentials
	// CredsBundle is the credentials bundle to be used. Only one of
	// TransportCredentials and CredsBundle is non-nil.
	CredsBundle credentials.Bundle
	// KeepaliveParams stores the keepalive parameters.
	KeepaliveParams keepalive.ClientParameters
	// InitialWindowSize sets the initial window size for a stream.
	InitialWindowSize int32
	// InitialConnWindowSize sets the initial window size for a connection.
	InitialConnWindowSize int32
	// WriteBufferSize sets the size of write buffer which in turn determines how much data can be batched before it's written on the wire.
	WriteBufferSize int
	// ReadBufferSize sets the size of read buffer, which in turn determines how much data can be read at most for one read syscall.
	ReadBufferSize int
	// ChannelzParentID sets the addrConn id which initiate the creation of this client transport.
	ChannelzParentID int64
	// MaxHeaderListSize sets the max (uncompressed) size of header list that is prepared to be received.
	MaxHeaderListSize *uint32
}

func NewClientTransport(connectCtx, ctx context.Context, addr resolver.Address, opts ConnectOptions, onPrefaceReceipt func(), onGoAway func(GoAwayReason), onClose func()) (ClientTransport, error) {
	return newHTTP2Client(connectCtx, ctx, addr, opts, onPrefaceReceipt, onGoAway, onClose)
}

type Options struct {
	// 表明写是否是最后一个分片
	Last bool
}

// CallHdr carries the information of a particular RPC.
type CallHdr struct {
	// Host specifies the peer's host.
	Host string

	// Method specifies the operation to perform.
	Method string

	// SendCompress specifies the compression algorithm applied on
	// outbound message.
	SendCompress string

	// Creds specifies credentials.PerRPCCredentials for a call.
	Creds credentials.PerRPCCredentials

	// ContentSubtype specifies the content-subtype for a request. For example, a
	// content-subtype of "proto" will result in a content-type of
	// "application/grpc+proto". The value of ContentSubtype must be all
	// lowercase, otherwise the behavior is undefined. See
	// https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#requests
	// for more details.
	ContentSubtype string

	PreviousAttempts int // value of grpc-previous-rpc-attempts header to set
}

type ClientTransport interface {
	// Close tears down this transport. Once it returns, the transport
	// should not be accessed any more. The caller must make sure this
	// is called only once.
	Close() error

	// GracefulClose starts to tear down the transport: the transport will stop
	// accepting new RPCs and NewStream will return error. Once all streams are
	// finished, the transport will close.
	//
	// It does not block.
	GracefulClose()

	// Write sends the data for the given stream. A nil stream indicates
	// the write is to be performed on the transport as a whole.
	Write(s *Stream, hdr []byte, data []byte, opts *Options) error

	// NewStream creates a Stream for an RPC.
	NewStream(ctx context.Context, callHdr *CallHdr) (*Stream, error)

	// CloseStream clears the footprint of a stream when the stream is
	// not needed any more. The err indicates the error incurred when
	// CloseStream is called. Must be called when a stream is finished
	// unless the associated transport is closing.
	CloseStream(stream *Stream, err error)

	// Error returns a channel that is closed when some I/O error
	// happens. Typically the caller should have a goroutine to monitor
	// this in order to take action (e.g., close the current transport
	// and create a new one) in error case. It should not return nil
	// once the transport is initiated.
	Error() <-chan struct{}

	// GoAway returns a channel that is closed when ClientTransport
	// receives the draining signal from the server (e.g., GOAWAY frame in
	// HTTP/2).
	GoAway() <-chan struct{}

	// GetGoAwayReason returns the reason why GoAway frame was received.
	GetGoAwayReason() GoAwayReason

	// RemoteAddr returns the remote network address.
	RemoteAddr() net.Addr

	// IncrMsgSent increments the number of message sent through this transport.
	IncrMsgSent()

	// IncrMsgRecv increments the number of message received through this transport.
	IncrMsgRecv()
}

type ServerTransport interface {
	HandleStreams(func(*Stream), func(context.Context, string) context.Context)
	WriteHeader(s *Stream, md metadata.MD) error
	Write(s *Stream, hdr []byte, data []byte, opts *Options) error
	WriteStatus(s *Stream, st *status.Status) error
	Close() error
	RemoteAddr() net.Addr
	Drain() // 通知客户端服务端已经停止接受流了
}

// connectionErrorf creates an ConnectionError with the specified error description.
func connectionErrorf(temp bool, e error, format string, a ...interface{}) ConnectionError {
	return ConnectionError{
		Desc: fmt.Sprintf(format, a...),
		temp: temp,
		err:  e,
	}
}

// ConnectionError is an error that results in the termination of the
// entire connection and the retry of all the active streams.
type ConnectionError struct {
	Desc string
	temp bool
	err  error
}

func (e ConnectionError) Error() string {
	return fmt.Sprintf("connection error: desc = %q", e.Desc)
}

// Temporary indicates if this connection error is temporary or fatal.
func (e ConnectionError) Temporary() bool {
	return e.temp
}

// Origin returns the original error of this connection error.
func (e ConnectionError) Origin() error {
	// Never return nil error here.
	// If the original error is nil, return itself.
	if e.err == nil {
		return e
	}
	return e.err
}

var (
	// ErrConnClosing indicates that the transport is closing.
	ErrConnClosing = connectionErrorf(true, nil, "transport is closing")
	// errStreamDrain indicates that the stream is rejected because the
	// connection is draining. This could be caused by goaway or balancer
	// removing the address.
	errStreamDrain = status.Error(codes.Unavailable, "the connection is draining")
	// errStreamDone is returned from write at the client side to indiacte application
	// layer of an error.
	errStreamDone = errors.New("the stream is done")
	// StatusGoAway indicates that the server sent a GOAWAY that included this
	// stream's ID in unprocessed RPCs.
	statusGoAway = status.New(codes.Unavailable, "the stream is rejected because server is draining the connection")
)

// GoAwayReason contains the reason for the GoAway frame received.
type GoAwayReason uint8

const (
	// GoAwayInvalid indicates that no GoAway frame is received.
	GoAwayInvalid GoAwayReason = 0
	// GoAwayNoReason is the default value when GoAway frame is received.
	GoAwayNoReason GoAwayReason = 1
	// GoAwayTooManyPings indicates that a GoAway frame with
	// ErrCodeEnhanceYourCalm was received and that the debug data said
	// "too_many_pings".
	GoAwayTooManyPings GoAwayReason = 2
)

// ContextErr converts the error from context package into a status error.
func ContextErr(err error) error {
	switch err {
	case context.DeadlineExceeded:
		return status.Error(codes.DeadlineExceeded, err.Error())
	case context.Canceled:
		return status.Error(codes.Canceled, err.Error())
	}
	return status.Errorf(codes.Internal, "Unexpected error from context packet: %v", err)
}
