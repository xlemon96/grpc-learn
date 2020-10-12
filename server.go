package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/internal/grpcrand"
	"google.golang.org/grpc/internal/grpcsync"
	"google.golang.org/grpc/internal/transport"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	defaultServerMaxReceiveMessageSize = 1024 * 1024 * 4
	defaultServerMaxSendMessageSize    = math.MaxInt32

	serverWorkerResetThreshold = 1 << 16
)

var (
	ErrServerStopped = errors.New("grpc: the server has been stopped")

	statusOK = status.New(codes.OK, "")

	logger = grpclog.Component("core")
)

type methodHandler func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor UnaryServerInterceptor) (interface{}, error)

// 对一个服务方法的描述
type MethodDesc struct {
	MethodName string        // 方法名字
	Handler    methodHandler // 方法执行器
}

// 一个服务的描述
// 给编译proto时候用
type ServiceDesc struct {
	ServiceName string      // 服务名
	HandlerType interface{} // 指向service，检查是否满足接口需求
	Methods     []MethodDesc
	Streams     []StreamDesc
	Metadata    interface{}
}

// grpc server内部用
type service struct {
	server   interface{}
	md       map[string]*MethodDesc
	sd       map[string]*StreamDesc
	metadata interface{}
}

type MethodInfo struct {
	Name           string // 就只有方法名字，不包含服务名和包名
	IsClientStream bool   // 是否为client
	IsServerStream bool   // 是否为服务端
}

type ServiceInfo struct {
	Methods  []MethodInfo
	Metadata interface{}
}

type serverWorkerData struct {
	st     transport.ServerTransport
	wg     *sync.WaitGroup
	stream *transport.Stream
}

// 服务器结构体
type Server struct {
	opts                 serverOptions                      // 服务器配置信息
	mu                   sync.Mutex                         // 锁
	lis                  map[net.Listener]bool              // listener管理
	conns                map[transport.ServerTransport]bool // conn管理
	serve                bool                               // 是否已经开启服务
	drain                bool                               // ？？？？？？
	cv                   *sync.Cond                         // 优雅退出的时候需要
	m                    map[string]*service                // 所有的服务
	quit                 *grpcsync.Event
	done                 *grpcsync.Event
	serveWG              sync.WaitGroup // 保证g优雅退出
	serverWorkerChannels []chan *serverWorkerData
}

func NewServer(opt ...ServerOption) *Server {
	opts := defaultServerOptions
	for _, o := range opt {
		o.apply(&opts)
	}
	s := &Server{
		lis:   make(map[net.Listener]bool),
		opts:  opts,
		conns: make(map[transport.ServerTransport]bool),
		m:     make(map[string]*service),
		quit:  grpcsync.NewEvent(),
		done:  grpcsync.NewEvent(),
	}
	chainUnaryServerInterceptors(s)
	chainStreamServerInterceptors(s)
	s.cv = sync.NewCond(&s.mu)
	if s.opts.numServerWorkers > 0 {
		s.initServerWorkers()
	}
	return s
}

// 注册一个service到grpc server
func (s *Server) RegisterService(sd *ServiceDesc, ss interface{}) {
	ht := reflect.TypeOf(sd.HandlerType).Elem()
	st := reflect.TypeOf(ss)
	if !st.Implements(ht) {
		logger.Fatalf("grpc: Server.RegisterService found the handler of type %v that does not satisfy %v", st, ht)
	}
	s.register(sd, ss)
}

// 获取服务信息
// 将m map转换为ServiceInfo map
func (s *Server) GetServiceInfo() map[string]ServiceInfo {
	ret := make(map[string]ServiceInfo)
	for n, srv := range s.m {
		methods := make([]MethodInfo, 0, len(srv.md)+len(srv.sd))
		for m := range srv.md {
			methods = append(methods, MethodInfo{
				Name:           m,
				IsClientStream: false,
				IsServerStream: false,
			})
		}
		for m, d := range srv.sd {
			methods = append(methods, MethodInfo{
				Name:           m,
				IsClientStream: d.ClientStreams,
				IsServerStream: d.ServerStreams,
			})
		}
		ret[n] = ServiceInfo{
			Methods:  methods,
			Metadata: srv.metadata,
		}
	}
	return ret
}

// grpc server启动
func (s *Server) Serve(lis net.Listener) error {
	s.mu.Lock()
	s.serve = true
	if s.lis == nil {
		// 此种情况是启动函数调用在server关闭之后
		s.mu.Unlock()
		lis.Close()
		return ErrServerStopped
	}
	// 通过serveWG确保所有的g都正常退出
	s.serveWG.Add(1)
	defer func() {
		s.serveWG.Done()
		if s.quit.HasFired() {
			<-s.done.Done()
		}
	}()
	s.lis[lis] = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.lis != nil && s.lis[lis] {
			lis.Close()
			delete(s.lis, lis)
		}
		s.mu.Unlock()
	}()
	var tempDelay time.Duration
	// 开启主循环，监听连接并处理
	for {
		rawConn, err := lis.Accept()
		if err != nil {
			if ne, ok := err.(interface {
				Temporary() bool
			}); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				timer := time.NewTimer(tempDelay)
				select {
				case <-timer.C:
				case <-s.quit.Done():
					timer.Stop()
					return nil
				}
				continue
			}
			if s.quit.HasFired() {
				return nil
			}
			return err
		}
		tempDelay = 0
		s.serveWG.Add(1)
		go func() {
			s.handleRawConn(rawConn)
			s.serveWG.Done()
		}()
	}
}

// 关闭服务器
func (s *Server) Stop() {
	s.quit.Fire()
	defer func() {
		s.serveWG.Wait()
		s.done.Fire()
	}()
	s.mu.Lock()
	listeners := s.lis
	s.lis = nil
	st := s.conns
	s.conns = nil
	// interrupt GracefulStop if Stop and GracefulStop are called concurrently.
	s.cv.Broadcast()
	s.mu.Unlock()

	for lis := range listeners {
		lis.Close()
	}
	for c := range st {
		c.Close()
	}
	if s.opts.numServerWorkers > 0 {
		s.stopServerWorkers()
	}
}

// 优雅关闭rpc服务器
func (s *Server) GracefulStop() {
	s.quit.Fire()
	defer s.done.Fire()
	s.mu.Lock()
	if s.conns == nil {
		s.mu.Unlock()
		return
	}
	for lis := range s.lis {
		lis.Close()
	}
	s.lis = nil
	if !s.drain {
		for st := range s.conns {
			st.Drain()
		}
		s.drain = true
	}
	// Wait for serving threads to be ready to exit.  Only then can we be sure no
	// new conns will be created.
	s.mu.Unlock()
	s.serveWG.Wait()
	s.mu.Lock()
	for len(s.conns) != 0 {
		s.cv.Wait()
	}
	s.conns = nil
	s.mu.Unlock()
}

// 本身g池化作用不明显，但是由于调用链较长，会导致g频繁的扩容
// 因此需要重复利用已经扩容过的g。又为了保证资源不会被一只占用，定时将g重置
func (s *Server) initServerWorkers() {
	s.serverWorkerChannels = make([]chan *serverWorkerData, s.opts.numServerWorkers)
	for i := uint32(0); i < s.opts.numServerWorkers; i++ {
		s.serverWorkerChannels[i] = make(chan *serverWorkerData)
		go s.serverWorker(s.serverWorkerChannels[i])
	}
}

func (s *Server) serverWorker(ch chan *serverWorkerData) {
	// 确保不会所有的g同时reset
	threshold := serverWorkerResetThreshold + grpcrand.Intn(serverWorkerResetThreshold)
	for completed := 0; completed < threshold; completed++ {
		data, ok := <-ch
		if !ok {
			return
		}
		s.handleStream(data.st, data.stream)
		data.wg.Done()
	}
	go s.serverWorker(ch)
}

func (s *Server) stopServerWorkers() {
	for i := uint32(0); i < s.opts.numServerWorkers; i++ {
		close(s.serverWorkerChannels[i])
	}
}

// 实际的注册函数
// 将用户实现的结构体和protobuf生成的结构体写入到server
func (s *Server) register(sd *ServiceDesc, ss interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.serve {
		logger.Fatalf("grpc: Server.RegisterService after Server.Serve for %q", sd.ServiceName)
	}
	if _, ok := s.m[sd.ServiceName]; ok {
		logger.Fatalf("grpc: Server.RegisterService found duplicate service registration for %q", sd.ServiceName)
	}
	srv := &service{
		server:   ss,
		md:       make(map[string]*MethodDesc),
		sd:       make(map[string]*StreamDesc),
		metadata: sd.Metadata,
	}
	for i := range sd.Methods {
		d := &sd.Methods[i]
		srv.md[d.MethodName] = d
	}
	for i := range sd.Streams {
		d := &sd.Streams[i]
		srv.sd[d.StreamName] = d
	}
	s.m[sd.ServiceName] = srv
}

// conn处理函数
func (s *Server) handleRawConn(rawConn net.Conn) {
	// 如果已经开始退出，则直接关闭连接并return
	if s.quit.HasFired() {
		rawConn.Close()
		return
	}
	rawConn.SetDeadline(time.Now().Add(s.opts.connectionTimeout))
	// 鉴权相关，先忽略
	conn, authInfo, err := s.useTransportAuthenticator(rawConn)
	if err != nil {
		// ErrConnDispatched means that the connection was dispatched away from
		// gRPC; those connections should be left open.
		if err != credentials.ErrConnDispatched {
			rawConn.Close()
		}
		rawConn.SetDeadline(time.Time{})
		return
	}
	// 完成握手
	st := s.newHTTP2Transport(conn, authInfo)
	if st == nil {
		return
	}
	rawConn.SetDeadline(time.Time{})
	if !s.addConn(st) {
		return
	}
	go func() {
		s.serveStreams(st)
		s.removeConn(st)
	}()
}

// 是否用了鉴权信息
func (s *Server) useTransportAuthenticator(rawConn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	if s.opts.creds == nil {
		return rawConn, nil, nil
	}
	return s.opts.creds.ServerHandshake(rawConn)
}

// 增加一个连接到server
func (s *Server) addConn(st transport.ServerTransport) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conns == nil {
		st.Close()
		return false
	}
	if s.drain {
		st.Drain()
	}
	s.conns[st] = true
	return true
}

// 删除一个连接
func (s *Server) removeConn(st transport.ServerTransport) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conns != nil {
		delete(s.conns, st)
		s.cv.Broadcast()
	}
}

// 封装tcp conn为http2的transport
func (s *Server) newHTTP2Transport(c net.Conn, authInfo credentials.AuthInfo) transport.ServerTransport {
	config := &transport.ServerConfig{
		MaxStreams:            s.opts.maxConcurrentStreams,
		AuthInfo:              authInfo,
		InTapHandle:           s.opts.inTapHandle,
		KeepaliveParams:       s.opts.keepaliveParams,
		KeepalivePolicy:       s.opts.keepalivePolicy,
		InitialWindowSize:     s.opts.initialWindowSize,
		InitialConnWindowSize: s.opts.initialConnWindowSize,
		WriteBufferSize:       s.opts.writeBufferSize,
		ReadBufferSize:        s.opts.readBufferSize,
		MaxHeaderListSize:     s.opts.maxHeaderListSize,
		HeaderTableSize:       s.opts.headerTableSize,
	}
	st, err := transport.NewServerTransport("http2", c, config)
	if err != nil {
		c.Close()
		return nil
	}
	return st
}

// 处理流信息
// 此函数会阻塞，直到连接关闭
// 函数内部会一直读取此链接的stream并进行处理
func (s *Server) serveStreams(st transport.ServerTransport) {
	defer st.Close()
	var wg sync.WaitGroup // 保证所有的g退出之后再退出
	var roundRobinCounter uint32
	// 此函数就是不停处理stream的函数
	st.HandleStreams(func(stream *transport.Stream) {
		wg.Add(1)
		// 若开启worker模式，则优先向worker池丢stream
		// 否则新起一个g处理stream
		if s.opts.numServerWorkers > 0 {
			data := &serverWorkerData{st: st, wg: &wg, stream: stream}
			select {
			case s.serverWorkerChannels[atomic.AddUint32(&roundRobinCounter, 1)%s.opts.numServerWorkers] <- data:
			default:
				go func() {
					defer wg.Done()
					s.handleStream(st, stream)
				}()
			}
		} else {
			go func() {
				defer wg.Done()
				s.handleStream(st, stream)
			}()
		}
	}, func(ctx context.Context, method string) context.Context {
		// 由于去掉了trance，所以删除了原先的代码，直接返回ctx
		return ctx
	})
	wg.Wait()
}

// 处理一个stream
func (s *Server) handleStream(t transport.ServerTransport, stream *transport.Stream) {
	sm := stream.Method()
	if sm != "" && sm[0] == '/' {
		sm = sm[1:]
	}
	pos := strings.LastIndex(sm, "/")
	if pos == -1 {
		errDesc := fmt.Sprintf("malformed method name: %q", stream.Method())
		t.WriteStatus(stream, status.New(codes.ResourceExhausted, errDesc))
		return
	}
	service := sm[:pos]
	method := sm[pos+1:]
	// 拿到注册的服务
	srv, knownService := s.m[service]
	if knownService {
		if md, ok := srv.md[method]; ok {
			// 调用普通接口
			s.processUnaryRPC(t, stream, srv, md)
			return
		}
		if sd, ok := srv.sd[method]; ok {
			// 调用流式接口
			s.processStreamingRPC(t, stream, srv, sd)
			return
		}
	}
	// Unknown service, or known server unknown method.
	if unknownDesc := s.opts.unknownStreamDesc; unknownDesc != nil {
		s.processStreamingRPC(t, stream, nil, unknownDesc)
		return
	}
	var errDesc string
	if !knownService {
		errDesc = fmt.Sprintf("unknown service %v", service)
	} else {
		errDesc = fmt.Sprintf("unknown method %v for service %v", method, service)
	}
	// 若未找到对应的方法记录，则写回status给客户端
	t.WriteStatus(stream, status.New(codes.Unimplemented, errDesc))
}

// 非流式接口的调用
func (s *Server) processUnaryRPC(t transport.ServerTransport, stream *transport.Stream, srv *service, md *MethodDesc) (err error) {
	var comp encoding.Compressor
	// 根据stream的压缩类型获取压缩器
	// 若找不到，则直接抛出异常
	if rc := stream.RecvCompress(); rc != "" && rc != encoding.Identity {
		comp = encoding.GetCompressor(rc)
		if comp == nil {
			st := status.Newf(codes.Unimplemented, "grpc: Decompressor is not installed for grpc-encoding %q", rc)
			t.WriteStatus(stream, st)
			return st.Err()
		}
	}
	// 接受并解压缩消息
	d, err := recvAndDecompress(&parser{r: stream}, stream, s.opts.maxReceiveMessageSize, comp)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			t.WriteStatus(stream, st)
		}
		return err
	}
	// 封装反序列化函数
	df := func(v interface{}) error {
		if err := s.getCodec(stream.ContentSubtype()).Unmarshal(d, v); err != nil {
			return status.Errorf(codes.Internal, "grpc: error unmarshalling request: %v", err)
		}
		return nil
	}
	ctx := NewContextWithServerTransportStream(stream.Context(), stream)
	// 调用注册方法并获取响应
	reply, appErr := md.Handler(srv.server, ctx, df, s.opts.unaryInt)
	if appErr != nil {
		appStatus, ok := status.FromError(appErr)
		if !ok {
			// 如果appErr不是一个gRpc status异常，转换
			appErr = status.Error(codes.Unknown, appErr.Error())
			appStatus, _ = status.FromError(appErr)
		}
		t.WriteStatus(stream, appStatus)
		return appErr
	}
	opts := &transport.Options{Last: true}
	// 发送响应
	if err := s.sendResponse(t, stream, reply, opts, comp); err != nil {
		if err == io.EOF {
			// 出现EOF代表整个流已经结束
			return err
		}
		if sts, ok := status.FromError(err); ok {
			t.WriteStatus(stream, sts)
		} else {
			switch st := err.(type) {
			case transport.ConnectionError:
			default:
				panic(fmt.Sprintf("grpc: Unexpected error (%T) from sendResponse: %v", st, st))
			}
		}
		return err
	}
	err = t.WriteStatus(stream, statusOK)
	return err
}

// 流式接口调用
func (s *Server) processStreamingRPC(t transport.ServerTransport, stream *transport.Stream, srv *service, sd *StreamDesc) (err error) {
	ctx := NewContextWithServerTransportStream(stream.Context(), stream)
	ss := &serverStream{
		ctx:                   ctx,
		t:                     t,
		s:                     stream,
		p:                     &parser{r: stream},
		codec:                 s.getCodec(stream.ContentSubtype()),
		maxReceiveMessageSize: s.opts.maxReceiveMessageSize,
		maxSendMessageSize:    s.opts.maxSendMessageSize,
	}
	if rc := stream.RecvCompress(); rc != "" && rc != encoding.Identity {
		ss.comp = encoding.GetCompressor(rc)
		if ss.comp == nil {
			st := status.Newf(codes.Unimplemented, "grpc: Decompressor is not installed for grpc-encoding %q", rc)
			t.WriteStatus(ss.s, st)
			return st.Err()
		}
	}
	var appErr error
	var server interface{}
	if srv != nil {
		server = srv.server
	}
	if s.opts.streamInt == nil {
		appErr = sd.Handler(server, ss)
	} else {
		info := &StreamServerInfo{
			FullMethod:     stream.Method(),
			IsClientStream: sd.ClientStreams,
			IsServerStream: sd.ServerStreams,
		}
		appErr = s.opts.streamInt(server, ss, info, sd.Handler)
	}
	if appErr != nil {
		appStatus, ok := status.FromError(appErr)
		if !ok {
			appStatus = status.New(codes.Unknown, appErr.Error())
			appErr = appStatus.Err()
		}
		t.WriteStatus(ss.s, appStatus)
		return appErr
	}
	err = t.WriteStatus(ss.s, statusOK)
	return err
}

// 发送响应
func (s *Server) sendResponse(t transport.ServerTransport, stream *transport.Stream, msg interface{}, opts *transport.Options, comp encoding.Compressor) error {
	// 序列化消息
	data, err := encode(s.getCodec(stream.ContentSubtype()), msg)
	if err != nil {
		return err
	}
	// 压缩消息
	compData, err := compress(data, comp)
	if err != nil {
		return err
	}
	// 封装消息头和消息体
	hdr, payload := msgHeader(data, compData)
	if len(payload) > s.opts.maxSendMessageSize {
		return status.Errorf(codes.ResourceExhausted, "grpc: trying to send message larger than max (%d vs. %d)", len(payload), s.opts.maxSendMessageSize)
	}
	// 向transport发送消息
	err = t.Write(stream, hdr, payload, opts)
	return err
}

// contentSubtype一定是小写，不允许返回空
// 默认用proto进行序列化
func (s *Server) getCodec(contentSubtype string) encoding.Codec {
	if s.opts.codec != nil {
		return s.opts.codec
	}
	if contentSubtype == "" {
		return encoding.GetCodec(proto.Name)
	}
	codec := encoding.GetCodec(contentSubtype)
	if codec == nil {
		return encoding.GetCodec(proto.Name)
	}
	return codec
}

// 构造流式接口的拦截器
func chainStreamServerInterceptors(s *Server) {
	interceptors := s.opts.chainStreamInts
	if s.opts.streamInt != nil {
		interceptors = append([]StreamServerInterceptor{s.opts.streamInt}, s.opts.chainStreamInts...)
	}

	var chainedInt StreamServerInterceptor
	if len(interceptors) == 0 {
		chainedInt = nil
	} else if len(interceptors) == 1 {
		chainedInt = interceptors[0]
	} else {
		chainedInt = func(srv interface{}, ss ServerStream, info *StreamServerInfo, handler StreamHandler) error {
			return interceptors[0](srv, ss, info, getChainStreamHandler(interceptors, 0, info, handler))
		}
	}
	s.opts.streamInt = chainedInt
}

func getChainStreamHandler(interceptors []StreamServerInterceptor, curr int, info *StreamServerInfo, finalHandler StreamHandler) StreamHandler {
	if curr == len(interceptors)-1 {
		return finalHandler
	}
	return func(srv interface{}, ss ServerStream) error {
		return interceptors[curr+1](srv, ss, info, getChainStreamHandler(interceptors, curr+1, info, finalHandler))
	}
}

// 构造非流式接口的拦截器
func chainUnaryServerInterceptors(s *Server) {
	interceptors := s.opts.chainUnaryInts
	if s.opts.unaryInt != nil {
		interceptors = append([]UnaryServerInterceptor{s.opts.unaryInt}, s.opts.chainUnaryInts...)
	}

	var chainedInt UnaryServerInterceptor
	if len(interceptors) == 0 {
		chainedInt = nil
	} else if len(interceptors) == 1 {
		chainedInt = interceptors[0]
	} else {
		chainedInt = func(ctx context.Context, req interface{}, info *UnaryServerInfo, handler UnaryHandler) (interface{}, error) {
			return interceptors[0](ctx, req, info, getChainUnaryHandler(interceptors, 0, info, handler))
		}
	}
	s.opts.unaryInt = chainedInt
}

func getChainUnaryHandler(interceptors []UnaryServerInterceptor, curr int, info *UnaryServerInfo, finalHandler UnaryHandler) UnaryHandler {
	if curr == len(interceptors)-1 {
		return finalHandler
	}
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		return interceptors[curr+1](ctx, req, info, getChainUnaryHandler(interceptors, curr+1, info, finalHandler))
	}
}

type streamKey struct{}

// 保存stream到ctx
func NewContextWithServerTransportStream(ctx context.Context, stream ServerTransportStream) context.Context {
	return context.WithValue(ctx, streamKey{}, stream)
}

type ServerTransportStream interface {
	Method() string
	SetHeader(md metadata.MD) error
	SendHeader(md metadata.MD) error
	SetTrailer(md metadata.MD) error
}

func ServerTransportStreamFromContext(ctx context.Context) ServerTransportStream {
	s, _ := ctx.Value(streamKey{}).(ServerTransportStream)
	return s
}

// 所有的metadata将要发送，当以下任何一种情况发生:
//  - grpc.SendHeader() 已经被调用
//  - 第一个response已经被发出
//  - 一个 rpc status 已经被发出(不论是error或者success)
func SetHeader(ctx context.Context, md metadata.MD) error {
	if md.Len() == 0 {
		return nil
	}
	stream := ServerTransportStreamFromContext(ctx)
	if stream == nil {
		return status.Errorf(codes.Internal, "grpc: failed to fetch the stream from the context %v", ctx)
	}
	return stream.SetHeader(md)
}

func SendHeader(ctx context.Context, md metadata.MD) error {
	stream := ServerTransportStreamFromContext(ctx)
	if stream == nil {
		return status.Errorf(codes.Internal, "grpc: failed to fetch the stream from the context %v", ctx)
	}
	if err := stream.SendHeader(md); err != nil {
		return toRPCErr(err)
	}
	return nil
}

func SetTrailer(ctx context.Context, md metadata.MD) error {
	if md.Len() == 0 {
		return nil
	}
	stream := ServerTransportStreamFromContext(ctx)
	if stream == nil {
		return status.Errorf(codes.Internal, "grpc: failed to fetch the stream from the context %v", ctx)
	}
	return stream.SetTrailer(md)
}

func Method(ctx context.Context) (string, bool) {
	s := ServerTransportStreamFromContext(ctx)
	if s == nil {
		return "", false
	}
	return s.Method(), true
}
