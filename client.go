package grpc

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/internal/backoff"
	"google.golang.org/grpc/internal/grpcsync"
	"google.golang.org/grpc/internal/grpcutil"
	"google.golang.org/grpc/internal/transport"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"google.golang.org/grpc/status"

	_ "google.golang.org/grpc/balancer/roundrobin"           // To register round robin.
	_ "google.golang.org/grpc/internal/resolver/dns"         // To register dns resolver.
	_ "google.golang.org/grpc/internal/resolver/passthrough" // To register pass through resolver.
)

const (
	// minimum time to give a connection to complete
	minConnectTimeout = 20 * time.Second
	// must match grpclbName in grpclb/grpclb.go
	grpclbName = "grpclb"
)

var (
	// ErrClientConnClosing indicates that the operation is illegal because
	// the ClientConn is closing.
	//
	// Deprecated: this error should not be relied upon by users; use the status
	// code of Canceled instead.
	ErrClientConnClosing = status.Error(codes.Canceled, "grpc: the client connection is closing")
	errConnDrain         = errors.New("grpc: the connection is drained") // 表明客户端不再接受任何rpc
	errConnClosing       = errors.New("grpc: the connection is closing") // 表明客户端已经关闭
	// invalidDefaultServiceConfigErrPrefix is used to prefix the json parsing error for the default
	// service config.
	invalidDefaultServiceConfigErrPrefix = "grpc: the provided default service config is invalid"
)

// ErrClientConnTimeout indicates that the ClientConn cannot establish the
// underlying connections within the specified timeout.
//
// Deprecated: This error is never returned by grpc and should not be
// referenced by users.
var ErrClientConnTimeout = errors.New("grpc: timed out when dialing")

// The following errors are returned from Dial and DialContext
var (
	// errNoTransportSecurity indicates that there is no transport security
	// being set for ClientConn. Users should either set one or explicitly
	// call WithInsecure DialOption to disable security.
	errNoTransportSecurity = errors.New("grpc: no transport security set (use grpc.WithInsecure() explicitly or set credentials)")
	// errTransportCredsAndBundle indicates that creds bundle is used together
	// with other individual Transport Credentials.
	errTransportCredsAndBundle = errors.New("grpc: credentials.Bundle may not be used with individual TransportCredentials")
	// errTransportCredentialsMissing indicates that users want to transmit security
	// information (e.g., OAuth2 token) which requires secure connection on an insecure
	// connection.
	errTransportCredentialsMissing = errors.New("grpc: the credentials require transport level security (use grpc.WithTransportCredentials() to set)")
	// errCredentialsConflict indicates that grpc.WithTransportCredentials()
	// and grpc.WithInsecure() are both called for a connection.
	errCredentialsConflict = errors.New("grpc: transport credentials are set for an insecure connection (grpc.WithTransportCredentials() and grpc.WithInsecure() are both called)")
)

const (
	defaultClientMaxReceiveMessageSize = 1024 * 1024 * 4
	defaultClientMaxSendMessageSize    = math.MaxInt32
	// http2IOBufSize specifies the buffer size for sending frames.
	defaultWriteBufSize = 32 * 1024
	defaultReadBufSize  = 32 * 1024
)

var unaryStreamDesc = &StreamDesc{ServerStreams: false, ClientStreams: false}

var emptyServiceConfig *ServiceConfig

func init() {
	cfg := parseServiceConfig("{}")
	if cfg.Err != nil {
		panic(fmt.Sprintf("impossible error parsing empty service config: %v", cfg.Err))
	}
	emptyServiceConfig = cfg.Config.(*ServiceConfig)
}

func Dial(target string, opts ...DialOption) (*ClientConn, error) {
	return DialContext(context.Background(), target, opts...)
}

// 获取一个ClientConn
func DialContext(ctx context.Context, target string, opts ...DialOption) (conn *ClientConn, err error) {
	cc := &ClientConn{
		target:            target,
		csMgr:             &connectivityStateManager{},
		conns:             make(map[*addrConn]struct{}),
		opts:              defaultDialOptions(),
		blockingpicker:    newPickerWrapper(),
		firstResolveEvent: grpcsync.NewEvent(),
	}
	cc.retryThrottler.Store((*retryThrottler)(nil))
	cc.ctx, cc.cancel = context.WithCancel(context.Background())
	for _, opt := range opts {
		opt.apply(&cc.opts)
	}
	chainUnaryClientInterceptors(cc)
	chainStreamClientInterceptors(cc)
	defer func() {
		if err != nil {
			cc.Close()
		}
	}()
	if !cc.opts.insecure {
		if cc.opts.copts.TransportCredentials == nil && cc.opts.copts.CredsBundle == nil {
			return nil, errNoTransportSecurity
		}
		if cc.opts.copts.TransportCredentials != nil && cc.opts.copts.CredsBundle != nil {
			return nil, errTransportCredsAndBundle
		}
	} else {
		if cc.opts.copts.TransportCredentials != nil || cc.opts.copts.CredsBundle != nil {
			return nil, errCredentialsConflict
		}
		for _, cd := range cc.opts.copts.PerRPCCredentials {
			if cd.RequireTransportSecurity() {
				return nil, errTransportCredentialsMissing
			}
		}
	}
	if cc.opts.defaultServiceConfigRawJSON != nil {
		sc := parseServiceConfig(*cc.opts.defaultServiceConfigRawJSON)
		if sc.Err != nil {
			return nil, fmt.Errorf("%s: %v", invalidDefaultServiceConfigErrPrefix, sc.Err)
		}
		cc.opts.defaultServiceConfig, _ = sc.Config.(*ServiceConfig)
	}
	cc.mkp = cc.opts.copts.KeepaliveParams
	if cc.opts.copts.Dialer == nil {
		cc.opts.copts.Dialer = func(ctx context.Context, addr string) (net.Conn, error) {
			network, addr := parseDialTarget(addr)
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}
		if cc.opts.withProxy {
			cc.opts.copts.Dialer = newProxyDialer(cc.opts.copts.Dialer)
		}
	}
	if cc.opts.copts.UserAgent != "" {
		cc.opts.copts.UserAgent += " " + grpcUA
	} else {
		cc.opts.copts.UserAgent = grpcUA
	}
	if cc.opts.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cc.opts.timeout)
		defer cancel()
	}
	defer func() {
		select {
		case <-ctx.Done():
			switch {
			case ctx.Err() == err:
				conn = nil
			case err == nil || !cc.opts.returnLastError:
				conn, err = nil, ctx.Err()
			default:
				conn, err = nil, fmt.Errorf("%v: %v", ctx.Err(), err)
			}
		default:
		}
	}()
	scSet := false
	if cc.opts.scChan != nil {
		select {
		case sc, ok := <-cc.opts.scChan:
			if ok {
				cc.sc = &sc
				scSet = true
			}
		default:
		}
	}
	if cc.opts.bs == nil {
		cc.opts.bs = backoff.DefaultExponential
	}
	cc.parsedTarget = grpcutil.ParseTarget(cc.target)
	resolverBuilder := cc.getResolver(cc.parsedTarget.Scheme)
	if resolverBuilder == nil {
		// If resolver builder is still nil, the parsed target's scheme is
		// not registered. Fallback to default resolver and set Endpoint to
		// the original target.
		cc.parsedTarget = resolver.Target{
			Scheme:   resolver.GetDefaultScheme(),
			Endpoint: target,
		}
		resolverBuilder = cc.getResolver(cc.parsedTarget.Scheme)
		if resolverBuilder == nil {
			return nil, fmt.Errorf("could not get resolver for default scheme: %q", cc.parsedTarget.Scheme)
		}
	}
	tCredentials := cc.opts.copts.TransportCredentials
	if tCredentials != nil && tCredentials.Info().ServerName != "" {
		cc.authority = tCredentials.Info().ServerName
	} else if cc.opts.insecure && cc.opts.authority != "" {
		cc.authority = cc.opts.authority
	} else {
		// Use endpoint from "scheme://authority/endpoint" as the default
		// authority for ClientConn.
		cc.authority = cc.parsedTarget.Endpoint
	}
	if cc.opts.scChan != nil && !scSet {
		// Blocking wait for the initial service config.
		select {
		case sc, ok := <-cc.opts.scChan:
			if ok {
				cc.sc = &sc
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if cc.opts.scChan != nil {
		go cc.scWatcher()
	}
	var tcClone credentials.TransportCredentials
	if tc := cc.opts.copts.TransportCredentials; tc != nil {
		tcClone = tc.Clone()
	}
	cc.balancerBuildOpts = balancer.BuildOptions{
		DialCreds:   tcClone,
		CredsBundle: cc.opts.copts.CredsBundle,
		Dialer:      cc.opts.copts.Dialer,
		Target:      cc.parsedTarget,
	}
	rWrapper, err := newCCResolverWrapper(cc, resolverBuilder)
	if err != nil {
		return nil, fmt.Errorf("failed to build resolver: %v", err)
	}
	cc.mu.Lock()
	cc.resolverWrapper = rWrapper
	cc.mu.Unlock()
	// A blocking dial blocks until the clientConn is ready.
	if cc.opts.block {
		for {
			s := cc.GetState()
			if s == connectivity.Ready {
				break
			} else if cc.opts.copts.FailOnNonTempDialError && s == connectivity.TransientFailure {
				if err = cc.connectionError(); err != nil {
					terr, ok := err.(interface {
						Temporary() bool
					})
					if ok && !terr.Temporary() {
						return nil, err
					}
				}
			}
			if !cc.WaitForStateChange(ctx, s) {
				if err = cc.connectionError(); err != nil && cc.opts.returnLastError {
					return nil, err
				}
				return nil, ctx.Err()
			}
		}
	}
	return cc, nil
}

type ClientConnInterface interface {
	Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...CallOption) error
	NewStream(ctx context.Context, desc *StreamDesc, method string, opts ...CallOption) (ClientStream, error)
}

var _ ClientConnInterface = (*ClientConn)(nil)

type ClientConn struct {
	ctx                 context.Context           // ctx
	cancel              context.CancelFunc        // cancel
	target              string                    // 目标地址
	parsedTarget        resolver.Target           // 目标地址解析器
	authority           string                    // 鉴权信息
	opts                dialOptions               // 配置
	csMgr               *connectivityStateManager // 连接状态管理器
	balancerBuildOpts   balancer.BuildOptions     // 负载均衡构建配置
	blockingpicker      *pickerWrapper            //
	mu                  sync.RWMutex              // 锁
	resolverWrapper     *ccResolverWrapper
	sc                  *ServiceConfig
	conns               map[*addrConn]struct{}     // 连接管理
	mkp                 keepalive.ClientParameters // 保活
	curBalancerName     string                     // 当前负载均衡名字
	balancerWrapper     *ccBalancerWrapper         // 负载均衡包装器
	retryThrottler      atomic.Value               //
	firstResolveEvent   *grpcsync.Event            //
	lceMu               sync.Mutex                 // 保护lastConnectionError
	lastConnectionError error                      //
}

// NewStream creates a new Stream for the client side. This is typically
// called by generated code. ctx is used for the lifetime of the stream.
//
// To ensure resources are not leaked due to the stream returned, one of the following
// actions must be performed:
//
//      1. Call Close on the ClientConn.
//      2. Cancel the context provided.
//      3. Call RecvMsg until a non-nil error is returned. A protobuf-generated
//         client-streaming RPC, for instance, might use the helper function
//         CloseAndRecv (note that CloseSend does not Recv, therefore is not
//         guaranteed to release all resources).
//      4. Receive a non-nil, non-io.EOF error from Header or SendMsg.
//
// If none of the above happen, a goroutine and a context will be leaked, and grpc
// will not call the optionally-configured stats handler with a stats.End message.
func (cc *ClientConn) NewStream(ctx context.Context, desc *StreamDesc, method string, opts ...CallOption) (ClientStream, error) {
	opts = combine(cc.opts.callOptions, opts)
	if cc.opts.streamInt != nil {
		return cc.opts.streamInt(ctx, desc, cc, method, newClientStream, opts...)
	}
	return newClientStream(ctx, desc, cc, method, opts...)
}

func (cc *ClientConn) Invoke(ctx context.Context, method string, args, reply interface{}, opts ...CallOption) error {
	// allow interceptor to see all applicable call options, which means those
	// configured as defaults from dial option as well as per-call options
	opts = combine(cc.opts.callOptions, opts)

	if cc.opts.unaryInt != nil {
		return cc.opts.unaryInt(ctx, method, args, reply, cc, invoke, opts...)
	}
	return invoke(ctx, method, args, reply, cc, opts...)
}

// 等待连接状态变化
func (cc *ClientConn) WaitForStateChange(ctx context.Context, sourceState connectivity.State) bool {
	ch := cc.csMgr.getNotifyChan()
	if cc.csMgr.getState() != sourceState {
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case <-ch:
		return true
	}
}

// 获取连接状态
func (cc *ClientConn) GetState() connectivity.State {
	return cc.csMgr.getState()
}

// server config监听
func (cc *ClientConn) scWatcher() {
	for {
		select {
		case sc, ok := <-cc.opts.scChan:
			if !ok {
				return
			}
			cc.mu.Lock()
			cc.sc = &sc
			cc.mu.Unlock()
		case <-cc.ctx.Done():
			return
		}
	}
}

// 等待Resolver返回一个地址
func (cc *ClientConn) waitForResolvedAddrs(ctx context.Context) error {
	// This is on the RPC path, so we use a fast path to avoid the
	// more-expensive "select" below after the resolver has returned once.
	if cc.firstResolveEvent.HasFired() {
		return nil
	}
	select {
	case <-cc.firstResolveEvent.Done():
		return nil
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case <-cc.ctx.Done():
		return ErrClientConnClosing
	}
}

func (cc *ClientConn) maybeApplyDefaultServiceConfig(addrs []resolver.Address) {
	if cc.sc != nil {
		cc.applyServiceConfigAndBalancer(cc.sc, addrs)
		return
	}
	if cc.opts.defaultServiceConfig != nil {
		cc.applyServiceConfigAndBalancer(cc.opts.defaultServiceConfig, addrs)
	} else {
		cc.applyServiceConfigAndBalancer(emptyServiceConfig, addrs)
	}
}

func (cc *ClientConn) updateResolverState(s resolver.State, err error) error {
	defer cc.firstResolveEvent.Fire()
	cc.mu.Lock()
	if cc.conns == nil {
		cc.mu.Unlock()
		return nil
	}
	if err != nil {
		// May need to apply the initial service config in case the resolver
		// doesn't support service configs, or doesn't provide a service config
		// with the new addresses.
		cc.maybeApplyDefaultServiceConfig(nil)
		if cc.balancerWrapper != nil {
			cc.balancerWrapper.resolverError(err)
		}
		// No addresses are valid with err set; return early.
		cc.mu.Unlock()
		return balancer.ErrBadResolverState
	}
	var ret error
	if cc.opts.disableServiceConfig || s.ServiceConfig == nil {
		cc.maybeApplyDefaultServiceConfig(s.Addresses)
		// TODO: do we need to apply a failing LB policy if there is no
		// default, per the error handling design?
	} else {
		if sc, ok := s.ServiceConfig.Config.(*ServiceConfig); s.ServiceConfig.Err == nil && ok {
			cc.applyServiceConfigAndBalancer(sc, s.Addresses)
		} else {
			ret = balancer.ErrBadResolverState
			if cc.balancerWrapper == nil {
				var err error
				if s.ServiceConfig.Err != nil {
					err = status.Errorf(codes.Unavailable, "error parsing service config: %v", s.ServiceConfig.Err)
				} else {
					err = status.Errorf(codes.Unavailable, "illegal service config type: %T", s.ServiceConfig.Config)
				}
				cc.blockingpicker.updatePicker(base.NewErrPicker(err))
				cc.csMgr.updateState(connectivity.TransientFailure)
				cc.mu.Unlock()
				return ret
			}
		}
	}

	var balCfg serviceconfig.LoadBalancingConfig
	if cc.opts.balancerBuilder == nil && cc.sc != nil && cc.sc.lbConfig != nil {
		balCfg = cc.sc.lbConfig.cfg
	}

	cbn := cc.curBalancerName
	bw := cc.balancerWrapper
	cc.mu.Unlock()
	if cbn != grpclbName {
		// Filter any grpclb addresses since we don't have the grpclb balancer.
		for i := 0; i < len(s.Addresses); {
			if s.Addresses[i].Type == resolver.GRPCLB {
				copy(s.Addresses[i:], s.Addresses[i+1:])
				s.Addresses = s.Addresses[:len(s.Addresses)-1]
				continue
			}
			i++
		}
	}
	uccsErr := bw.updateClientConnState(&balancer.ClientConnState{ResolverState: s, BalancerConfig: balCfg})
	if ret == nil {
		ret = uccsErr // prefer ErrBadResolver state since any other error is
		// currently meaningless to the caller.
	}
	return ret
}

// switchBalancer starts the switching from current balancer to the balancer
// with the given name.
//
// It will NOT send the current address list to the new balancer. If needed,
// caller of this function should send address list to the new balancer after
// this function returns.
//
// Caller must hold cc.mu.
func (cc *ClientConn) switchBalancer(name string) {
	if strings.EqualFold(cc.curBalancerName, name) {
		return
	}
	if cc.balancerWrapper != nil {
		cc.balancerWrapper.close()
	}
	builder := balancer.Get(name)
	if builder == nil {
		builder = newPickfirstBuilder()
	} else {
	}
	cc.curBalancerName = builder.Name()
	cc.balancerWrapper = newCCBalancerWrapper(cc, builder, cc.balancerBuildOpts)
}

func (cc *ClientConn) handleSubConnStateChange(sc balancer.SubConn, s connectivity.State, err error) {
	cc.mu.Lock()
	if cc.conns == nil {
		cc.mu.Unlock()
		return
	}
	// TODO(bar switching) send updates to all balancer wrappers when balancer
	// gracefully switching is supported.
	cc.balancerWrapper.handleSubConnStateChange(sc, s, err)
	cc.mu.Unlock()
}

// newAddrConn creates an addrConn for addrs and adds it to cc.conns.
//
// Caller needs to make sure len(addrs) > 0.
func (cc *ClientConn) newAddrConn(addrs []resolver.Address, opts balancer.NewSubConnOptions) (*addrConn, error) {
	ac := &addrConn{
		state:        connectivity.Idle,
		cc:           cc,
		addrs:        addrs,
		scopts:       opts,
		opts:         cc.opts,
		resetBackoff: make(chan struct{}),
	}
	ac.ctx, ac.cancel = context.WithCancel(cc.ctx)
	// Track ac in cc. This needs to be done before any getTransport(...) is called.
	cc.mu.Lock()
	if cc.conns == nil {
		cc.mu.Unlock()
		return nil, ErrClientConnClosing
	}
	cc.conns[ac] = struct{}{}
	cc.mu.Unlock()
	return ac, nil
}

// removeAddrConn removes the addrConn in the subConn from clientConn.
// It also tears down the ac with the given error.
func (cc *ClientConn) removeAddrConn(ac *addrConn, err error) {
	cc.mu.Lock()
	if cc.conns == nil {
		cc.mu.Unlock()
		return
	}
	delete(cc.conns, ac)
	cc.mu.Unlock()
	ac.tearDown(err)
}

// Target returns the target string of the ClientConn.
// This is an EXPERIMENTAL API.
func (cc *ClientConn) Target() string {
	return cc.target
}

// GetMethodConfig gets the method config of the input method.
// If there's an exact match for input method (i.e. /service/method), we return
// the corresponding MethodConfig.
// If there isn't an exact match for the input method, we look for the service's default
// config under the service (i.e /service/) and then for the default for all services (empty string).
//
// If there is a default MethodConfig for the service, we return it.
// Otherwise, we return an empty MethodConfig.
func (cc *ClientConn) GetMethodConfig(method string) MethodConfig {
	// TODO: Avoid the locking here.
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	if cc.sc == nil {
		return MethodConfig{}
	}
	if m, ok := cc.sc.Methods[method]; ok {
		return m
	}
	i := strings.LastIndex(method, "/")
	if m, ok := cc.sc.Methods[method[:i+1]]; ok {
		return m
	}
	return cc.sc.Methods[""]
}

func (cc *ClientConn) healthCheckConfig() *healthCheckConfig {
	cc.mu.RLock()
	defer cc.mu.RUnlock()
	if cc.sc == nil {
		return nil
	}
	return cc.sc.healthCheckConfig
}

func (cc *ClientConn) getTransport(ctx context.Context, failfast bool, method string) (transport.ClientTransport, func(balancer.DoneInfo), error) {
	t, done, err := cc.blockingpicker.pick(ctx, failfast, balancer.PickInfo{
		Ctx:            ctx,
		FullMethodName: method,
	})
	if err != nil {
		return nil, nil, toRPCErr(err)
	}
	return t, done, nil
}

func (cc *ClientConn) applyServiceConfigAndBalancer(sc *ServiceConfig, addrs []resolver.Address) {
	if sc == nil {
		// should never reach here.
		return
	}
	cc.sc = sc

	if cc.sc.retryThrottling != nil {
		newThrottler := &retryThrottler{
			tokens: cc.sc.retryThrottling.MaxTokens,
			max:    cc.sc.retryThrottling.MaxTokens,
			thresh: cc.sc.retryThrottling.MaxTokens / 2,
			ratio:  cc.sc.retryThrottling.TokenRatio,
		}
		cc.retryThrottler.Store(newThrottler)
	} else {
		cc.retryThrottler.Store((*retryThrottler)(nil))
	}

	if cc.opts.balancerBuilder == nil {
		// Only look at balancer types and switch balancer if balancer dial
		// option is not set.
		var newBalancerName string
		if cc.sc != nil && cc.sc.lbConfig != nil {
			newBalancerName = cc.sc.lbConfig.name
		} else {
			var isGRPCLB bool
			for _, a := range addrs {
				if a.Type == resolver.GRPCLB {
					isGRPCLB = true
					break
				}
			}
			if isGRPCLB {
				newBalancerName = grpclbName
			} else if cc.sc != nil && cc.sc.LB != nil {
				newBalancerName = *cc.sc.LB
			} else {
				newBalancerName = PickFirstBalancerName
			}
		}
		cc.switchBalancer(newBalancerName)
	} else if cc.balancerWrapper == nil {
		// Balancer dial option was set, and this is the first time handling
		// resolved addresses. Build a balancer with opts.balancerBuilder.
		cc.curBalancerName = cc.opts.balancerBuilder.Name()
		cc.balancerWrapper = newCCBalancerWrapper(cc, cc.opts.balancerBuilder, cc.balancerBuildOpts)
	}
}

func (cc *ClientConn) resolveNow(o resolver.ResolveNowOptions) {
	cc.mu.RLock()
	r := cc.resolverWrapper
	cc.mu.RUnlock()
	if r == nil {
		return
	}
	go r.resolveNow(o)
}

// ResetConnectBackoff wakes up all subchannels in transient failure and causes
// them to attempt another connection immediately.  It also resets the backoff
// times used for subsequent attempts regardless of the current state.
//
// In general, this function should not be used.  Typical service or network
// outages result in a reasonable client reconnection strategy by default.
// However, if a previously unavailable network becomes available, this may be
// used to trigger an immediate reconnect.
//
// This API is EXPERIMENTAL.
func (cc *ClientConn) ResetConnectBackoff() {
	cc.mu.Lock()
	conns := cc.conns
	cc.mu.Unlock()
	for ac := range conns {
		ac.resetConnectBackoff()
	}
}

// Close tears down the ClientConn and all underlying connections.
func (cc *ClientConn) Close() error {
	defer cc.cancel()

	cc.mu.Lock()
	if cc.conns == nil {
		cc.mu.Unlock()
		return ErrClientConnClosing
	}
	conns := cc.conns
	cc.conns = nil
	cc.csMgr.updateState(connectivity.Shutdown)

	rWrapper := cc.resolverWrapper
	cc.resolverWrapper = nil
	bWrapper := cc.balancerWrapper
	cc.balancerWrapper = nil
	cc.mu.Unlock()

	cc.blockingpicker.close()

	if rWrapper != nil {
		rWrapper.close()
	}
	if bWrapper != nil {
		bWrapper.close()
	}

	for ac := range conns {
		ac.tearDown(ErrClientConnClosing)
	}
	return nil
}

// 获取给定scheme的resolver.Builder
func (cc *ClientConn) getResolver(scheme string) resolver.Builder {
	for _, rb := range cc.opts.resolvers {
		if scheme == rb.Scheme() {
			return rb
		}
	}
	return resolver.Get(scheme)
}

// 更新lastConnectionError
func (cc *ClientConn) updateConnectionError(err error) {
	cc.lceMu.Lock()
	cc.lastConnectionError = err
	cc.lceMu.Unlock()
}

// 返回lastConnectionError
func (cc *ClientConn) connectionError() error {
	cc.lceMu.Lock()
	defer cc.lceMu.Unlock()
	return cc.lastConnectionError
}

// 封装一个net.Conn
type addrConn struct {
	ctx    context.Context    // ctx
	cancel context.CancelFunc // CancelFunc
	cc     *ClientConn        // 客户端连接管理器
	opts   dialOptions        // opts
	acbw   balancer.SubConn
	scopts balancer.NewSubConnOptions
	// transport is set when there's a viable transport (note: ac state may not be READY as LB channel
	// health checking may require server to report healthy to set ac to READY), and is reset
	// to nil when the current transport should no longer be used to create a stream (e.g. after GoAway
	// is received, transport is closed, ac has been torn down).
	transport    transport.ClientTransport // conn对应的transport
	mu           sync.Mutex                // 锁
	curAddr      resolver.Address          // 当前地址
	addrs        []resolver.Address        // 所有地址
	state        connectivity.State        // 连接状态管理
	backoffIdx   int                       // Needs to be stateful for resetConnectBackoff.
	resetBackoff chan struct{}
}

// adjustParams updates parameters used to create transports upon
// receiving a GoAway.
func (ac *addrConn) adjustParams(r transport.GoAwayReason) {
	switch r {
	case transport.GoAwayTooManyPings:
		v := 2 * ac.opts.copts.KeepaliveParams.Time
		ac.cc.mu.Lock()
		if v > ac.cc.mkp.Time {
			ac.cc.mkp.Time = v
		}
		ac.cc.mu.Unlock()
	}
}

// startHealthCheck starts the health checking stream (RPC) to watch the health
// stats of this connection if health checking is requested and configured.
//
// LB channel health checking is enabled when all requirements below are met:
// 1. it is not disabled by the user with the WithDisableHealthCheck DialOption
// 2. internal.HealthCheckFunc is set by importing the grpc/health package
// 3. a service config with non-empty healthCheckConfig field is provided
// 4. the load balancer requests it
//
// It sets addrConn to READY if the health checking stream is not started.
//
// Caller must hold ac.mu.
func (ac *addrConn) startHealthCheck(ctx context.Context) {
	var healthcheckManagingState bool
	defer func() {
		if !healthcheckManagingState {
			ac.updateConnectivityState(connectivity.Ready, nil)
		}
	}()

	if ac.cc.opts.disableHealthCheck {
		return
	}
	healthCheckConfig := ac.cc.healthCheckConfig()
	if healthCheckConfig == nil {
		return
	}
	if !ac.scopts.HealthCheckEnabled {
		return
	}
	healthCheckFunc := ac.cc.opts.healthCheckFunc
	if healthCheckFunc == nil {
		// The health package is not imported to set health check function.
		//
		// TODO: add a link to the health check doc in the error message.
		return
	}

	healthcheckManagingState = true

	// Set up the health check helper functions.
	currentTr := ac.transport
	newStream := func(method string) (interface{}, error) {
		ac.mu.Lock()
		if ac.transport != currentTr {
			ac.mu.Unlock()
			return nil, status.Error(codes.Canceled, "the provided transport is no longer valid to use")
		}
		ac.mu.Unlock()
		return newNonRetryClientStream(ctx, &StreamDesc{ServerStreams: true}, method, currentTr, ac)
	}
	setConnectivityState := func(s connectivity.State, lastErr error) {
		ac.mu.Lock()
		defer ac.mu.Unlock()
		if ac.transport != currentTr {
			return
		}
		ac.updateConnectivityState(s, lastErr)
	}
	// Start the health checking stream.
	go func() {
		err := ac.cc.opts.healthCheckFunc(ctx, newStream, setConnectivityState, healthCheckConfig.ServiceName)
		if err != nil {
			// todo
		}
	}()
}

func (ac *addrConn) resetConnectBackoff() {
	ac.mu.Lock()
	close(ac.resetBackoff)
	ac.backoffIdx = 0
	ac.resetBackoff = make(chan struct{})
	ac.mu.Unlock()
}

func (ac *addrConn) getReadyTransport() (transport.ClientTransport, bool) {
	ac.mu.Lock()
	if ac.state == connectivity.Ready && ac.transport != nil {
		t := ac.transport
		ac.mu.Unlock()
		return t, true
	}
	var idle bool
	if ac.state == connectivity.Idle {
		idle = true
	}
	ac.mu.Unlock()
	if idle {
		ac.connect()
	}
	return nil, false
}

// 获取一个transport
func (ac *addrConn) connect() error {
	ac.mu.Lock()
	if ac.state == connectivity.Shutdown {
		ac.mu.Unlock()
		return errConnClosing
	}
	if ac.state != connectivity.Idle {
		ac.mu.Unlock()
		return nil
	}
	ac.updateConnectivityState(connectivity.Connecting, nil)
	ac.mu.Unlock()
	go ac.resetTransport()
	return nil
}

func (ac *addrConn) resetTransport() {
	for i := 0; ; i++ {
		if i > 0 {
			ac.cc.resolveNow(resolver.ResolveNowOptions{})
		}
		ac.mu.Lock()
		if ac.state == connectivity.Shutdown {
			ac.mu.Unlock()
			return
		}
		addrs := ac.addrs
		backoffFor := ac.opts.bs.Backoff(ac.backoffIdx)
		// This will be the duration that dial gets to finish.
		dialDuration := minConnectTimeout
		if ac.opts.minConnectTimeout != nil {
			dialDuration = ac.opts.minConnectTimeout()
		}
		if dialDuration < backoffFor {
			// Give dial more time as we keep failing to connect.
			dialDuration = backoffFor
		}
		// We can potentially spend all the time trying the first address, and
		// if the server accepts the connection and then hangs, the following
		// addresses will never be tried.
		//
		// The spec doesn't mention what should be done for multiple addresses.
		// https://github.com/grpc/grpc/blob/master/doc/connection-backoff.md#proposed-backoff-algorithm
		connectDeadline := time.Now().Add(dialDuration)
		ac.updateConnectivityState(connectivity.Connecting, nil)
		ac.transport = nil
		ac.mu.Unlock()
		newTr, addr, reconnect, err := ac.tryAllAddrs(addrs, connectDeadline)
		if err != nil {
			// After exhausting all addresses, the addrConn enters
			// TRANSIENT_FAILURE.
			ac.mu.Lock()
			if ac.state == connectivity.Shutdown {
				ac.mu.Unlock()
				return
			}
			ac.updateConnectivityState(connectivity.TransientFailure, err)
			// Backoff.
			b := ac.resetBackoff
			ac.mu.Unlock()
			timer := time.NewTimer(backoffFor)
			select {
			case <-timer.C:
				ac.mu.Lock()
				ac.backoffIdx++
				ac.mu.Unlock()
			case <-b:
				timer.Stop()
			case <-ac.ctx.Done():
				timer.Stop()
				return
			}
			continue
		}
		ac.mu.Lock()
		if ac.state == connectivity.Shutdown {
			ac.mu.Unlock()
			newTr.Close()
			return
		}
		ac.curAddr = addr
		ac.transport = newTr
		ac.backoffIdx = 0
		hctx, hcancel := context.WithCancel(ac.ctx)
		ac.startHealthCheck(hctx)
		ac.mu.Unlock()
		// Block until the created transport is down. And when this happens,
		// we restart from the top of the addr list.
		<-reconnect.Done()
		hcancel()
		// restart connecting - the top of the loop will set state to
		// CONNECTING.  This is against the current connectivity semantics doc,
		// however it allows for graceful behavior for RPCs not yet dispatched
		// - unfortunate timing would otherwise lead to the RPC failing even
		// though the TRANSIENT_FAILURE state (called for by the doc) would be
		// instantaneous.
		//
		// Ideally we should transition to Idle here and block until there is
		// RPC activity that leads to the balancer requesting a reconnect of
		// the associated SubConn.
	}
}

// tryAllAddrs tries to creates a connection to the addresses, and stop when at the
// first successful one. It returns the transport, the address and a Event in
// the successful case. The Event fires when the returned transport disconnects.
func (ac *addrConn) tryAllAddrs(addrs []resolver.Address, connectDeadline time.Time) (transport.ClientTransport, resolver.Address, *grpcsync.Event, error) {
	var firstConnErr error
	for _, addr := range addrs {
		ac.mu.Lock()
		if ac.state == connectivity.Shutdown {
			ac.mu.Unlock()
			return nil, resolver.Address{}, nil, errConnClosing
		}
		ac.cc.mu.RLock()
		ac.opts.copts.KeepaliveParams = ac.cc.mkp
		ac.cc.mu.RUnlock()
		copts := ac.opts.copts
		if ac.scopts.CredsBundle != nil {
			copts.CredsBundle = ac.scopts.CredsBundle
		}
		ac.mu.Unlock()
		newTr, reconnect, err := ac.createTransport(addr, copts, connectDeadline)
		if err == nil {
			return newTr, addr, reconnect, nil
		}
		if firstConnErr == nil {
			firstConnErr = err
		}
		ac.cc.updateConnectionError(err)
	}
	// Couldn't connect to any address.
	return nil, resolver.Address{}, nil, firstConnErr
}

// createTransport creates a connection to addr. It returns the transport and a
// Event in the successful case. The Event fires when the returned transport
// disconnects.
func (ac *addrConn) createTransport(addr resolver.Address, copts transport.ConnectOptions, connectDeadline time.Time) (transport.ClientTransport, *grpcsync.Event, error) {
	prefaceReceived := make(chan struct{})
	onCloseCalled := make(chan struct{})
	reconnect := grpcsync.NewEvent()

	// addr.ServerName takes precedent over ClientConn authority, if present.
	if addr.ServerName == "" {
		addr.ServerName = ac.cc.authority
	}

	once := sync.Once{}
	onGoAway := func(r transport.GoAwayReason) {
		ac.mu.Lock()
		ac.adjustParams(r)
		once.Do(func() {
			if ac.state == connectivity.Ready {
				// Prevent this SubConn from being used for new RPCs by setting its
				// state to Connecting.
				//
				// TODO: this should be Idle when grpc-go properly supports it.
				ac.updateConnectivityState(connectivity.Connecting, nil)
			}
		})
		ac.mu.Unlock()
		reconnect.Fire()
	}

	onClose := func() {
		ac.mu.Lock()
		once.Do(func() {
			if ac.state == connectivity.Ready {
				// Prevent this SubConn from being used for new RPCs by setting its
				// state to Connecting.
				//
				// TODO: this should be Idle when grpc-go properly supports it.
				ac.updateConnectivityState(connectivity.Connecting, nil)
			}
		})
		ac.mu.Unlock()
		close(onCloseCalled)
		reconnect.Fire()
	}

	onPrefaceReceipt := func() {
		close(prefaceReceived)
	}

	connectCtx, cancel := context.WithDeadline(ac.ctx, connectDeadline)
	defer cancel()

	newTr, err := transport.NewClientTransport(connectCtx, ac.cc.ctx, addr, copts, onPrefaceReceipt, onGoAway, onClose)
	if err != nil {
		return nil, nil, err
	}

	select {
	case <-time.After(time.Until(connectDeadline)):
		// We didn't get the preface in time.
		newTr.Close()
		return nil, nil, errors.New("timed out waiting for server handshake")
	case <-prefaceReceived:
		// We got the preface - huzzah! things are good.
	case <-onCloseCalled:
		// The transport has already closed - noop.
		return nil, nil, errors.New("connection closed")
		// TODO(deklerk) this should bail on ac.ctx.Done(). Add a test and fix.
	}
	return newTr, reconnect, nil
}

// tryUpdateAddrs tries to update ac.addrs with the new addresses list.
//
// If ac is Connecting, it returns false. The caller should tear down the ac and
// create a new one. Note that the backoff will be reset when this happens.
//
// If ac is TransientFailure, it updates ac.addrs and returns true. The updated
// addresses will be picked up by retry in the next iteration after backoff.
//
// If ac is Shutdown or Idle, it updates ac.addrs and returns true.
//
// If ac is Ready, it checks whether current connected address of ac is in the
// new addrs list.
//  - If true, it updates ac.addrs and returns true. The ac will keep using
//    the existing connection.
//  - If false, it does nothing and returns false.
func (ac *addrConn) tryUpdateAddrs(addrs []resolver.Address) bool {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	if ac.state == connectivity.Shutdown ||
		ac.state == connectivity.TransientFailure ||
		ac.state == connectivity.Idle {
		ac.addrs = addrs
		return true
	}

	if ac.state == connectivity.Connecting {
		return false
	}

	// ac.state is Ready, try to find the connected address.
	var curAddrFound bool
	for _, a := range addrs {
		if reflect.DeepEqual(ac.curAddr, a) {
			curAddrFound = true
			break
		}
	}
	if curAddrFound {
		ac.addrs = addrs
	}

	return curAddrFound
}

// tearDown starts to tear down the addrConn.
// TODO(zhaoq): Make this synchronous to avoid unbounded memory consumption in
// some edge cases (e.g., the caller opens and closes many addrConn's in a
// tight loop.
// tearDown doesn't remove ac from ac.cc.conns.
func (ac *addrConn) tearDown(err error) {
	ac.mu.Lock()
	if ac.state == connectivity.Shutdown {
		ac.mu.Unlock()
		return
	}
	curTr := ac.transport
	ac.transport = nil
	// We have to set the state to Shutdown before anything else to prevent races
	// between setting the state and logic that waits on context cancellation / etc.
	ac.updateConnectivityState(connectivity.Shutdown, nil)
	ac.cancel()
	ac.curAddr = resolver.Address{}
	if err == errConnDrain && curTr != nil {
		// GracefulClose(...) may be executed multiple times when
		// i) receiving multiple GoAway frames from the server; or
		// ii) there are concurrent name resolver/Balancer triggered
		// address removal and GoAway.
		// We have to unlock and re-lock here because GracefulClose => Close => onClose, which requires locking ac.mu.
		ac.mu.Unlock()
		curTr.GracefulClose()
		ac.mu.Lock()
	}
	ac.mu.Unlock()
}

// 需要上锁
func (ac *addrConn) updateConnectivityState(s connectivity.State, lastErr error) {
	if ac.state == s {
		return
	}
	ac.state = s
	ac.cc.handleSubConnStateChange(ac.acbw, s, lastErr)
}

func (ac *addrConn) getState() connectivity.State {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	return ac.state
}

type retryThrottler struct {
	max    float64
	thresh float64
	ratio  float64
	mu     sync.Mutex
	tokens float64
}

func (rt *retryThrottler) throttle() bool {
	if rt == nil {
		return false
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.tokens--
	if rt.tokens < 0 {
		rt.tokens = 0
	}
	return rt.tokens <= rt.thresh
}

func (rt *retryThrottler) successfulRPC() {
	if rt == nil {
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.tokens += rt.ratio
	if rt.tokens > rt.max {
		rt.tokens = rt.max
	}
}

type connectivityStateManager struct {
	mu         sync.Mutex
	state      connectivity.State
	notifyChan chan struct{}
}

func (csm *connectivityStateManager) updateState(state connectivity.State) {
	csm.mu.Lock()
	defer csm.mu.Unlock()
	if csm.state == connectivity.Shutdown {
		return
	}
	if csm.state == state {
		return
	}
	csm.state = state
	if csm.notifyChan != nil {
		close(csm.notifyChan)
		csm.notifyChan = nil
	}
}

func (csm *connectivityStateManager) getState() connectivity.State {
	csm.mu.Lock()
	defer csm.mu.Unlock()
	return csm.state
}

func (csm *connectivityStateManager) getNotifyChan() <-chan struct{} {
	csm.mu.Lock()
	defer csm.mu.Unlock()
	if csm.notifyChan == nil {
		csm.notifyChan = make(chan struct{})
	}
	return csm.notifyChan
}

// chainUnaryClientInterceptors chains all unary client interceptors into one.
func chainUnaryClientInterceptors(cc *ClientConn) {
	interceptors := cc.opts.chainUnaryInts
	// Prepend opts.unaryInt to the chaining interceptors if it exists, since unaryInt will
	// be executed before any other chained interceptors.
	if cc.opts.unaryInt != nil {
		interceptors = append([]UnaryClientInterceptor{cc.opts.unaryInt}, interceptors...)
	}
	var chainedInt UnaryClientInterceptor
	if len(interceptors) == 0 {
		chainedInt = nil
	} else if len(interceptors) == 1 {
		chainedInt = interceptors[0]
	} else {
		chainedInt = func(ctx context.Context, method string, req, reply interface{}, cc *ClientConn, invoker UnaryInvoker, opts ...CallOption) error {
			return interceptors[0](ctx, method, req, reply, cc, getChainUnaryInvoker(interceptors, 0, invoker), opts...)
		}
	}
	cc.opts.unaryInt = chainedInt
}

func getChainUnaryInvoker(interceptors []UnaryClientInterceptor, curr int, finalInvoker UnaryInvoker) UnaryInvoker {
	if curr == len(interceptors)-1 {
		return finalInvoker
	}
	return func(ctx context.Context, method string, req, reply interface{}, cc *ClientConn, opts ...CallOption) error {
		return interceptors[curr+1](ctx, method, req, reply, cc, getChainUnaryInvoker(interceptors, curr+1, finalInvoker), opts...)
	}
}

// chainStreamClientInterceptors chains all stream client interceptors into one.
func chainStreamClientInterceptors(cc *ClientConn) {
	interceptors := cc.opts.chainStreamInts
	// Prepend opts.streamInt to the chaining interceptors if it exists, since streamInt will
	// be executed before any other chained interceptors.
	if cc.opts.streamInt != nil {
		interceptors = append([]StreamClientInterceptor{cc.opts.streamInt}, interceptors...)
	}
	var chainedInt StreamClientInterceptor
	if len(interceptors) == 0 {
		chainedInt = nil
	} else if len(interceptors) == 1 {
		chainedInt = interceptors[0]
	} else {
		chainedInt = func(ctx context.Context, desc *StreamDesc, cc *ClientConn, method string, streamer Streamer, opts ...CallOption) (ClientStream, error) {
			return interceptors[0](ctx, desc, cc, method, getChainStreamer(interceptors, 0, streamer), opts...)
		}
	}
	cc.opts.streamInt = chainedInt
}

func getChainStreamer(interceptors []StreamClientInterceptor, curr int, finalStreamer Streamer) Streamer {
	if curr == len(interceptors)-1 {
		return finalStreamer
	}
	return func(ctx context.Context, desc *StreamDesc, cc *ClientConn, method string, opts ...CallOption) (ClientStream, error) {
		return interceptors[curr+1](ctx, desc, cc, method, getChainStreamer(interceptors, curr+1, finalStreamer), opts...)
	}
}

func invoke(ctx context.Context, method string, req, reply interface{}, cc *ClientConn, opts ...CallOption) error {
	cs, err := newClientStream(ctx, unaryStreamDesc, cc, method, opts...)
	if err != nil {
		return err
	}
	if err := cs.SendMsg(req); err != nil {
		return err
	}
	return cs.RecvMsg(reply)
}

func combine(o1 []CallOption, o2 []CallOption) []CallOption {
	// we don't use append because o1 could have extra capacity whose
	// elements would be overwritten, which could cause inadvertent
	// sharing (and race conditions) between concurrent calls
	if len(o1) == 0 {
		return o2
	} else if len(o2) == 0 {
		return o1
	}
	ret := make([]CallOption, len(o1)+len(o2))
	copy(ret, o1)
	copy(ret[len(o1):], o2)
	return ret
}
