package grpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"io/ioutil"
	"math"
	"net/url"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/internal/transport"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// The SupportPackageIsVersion variables are referenced from generated protocol
// buffer files to ensure compatibility with the gRPC version used.  The latest
// support package version is 6.
//
// Older versions are kept for compatibility. They may be removed if
// compatibility cannot be maintained.
//
// These constants should not be referenced from any other code.
const (
	SupportPackageIsVersion3 = true
	SupportPackageIsVersion4 = true
	SupportPackageIsVersion5 = true
	SupportPackageIsVersion6 = true
)

const grpcUA = "grpc-go/" + Version

const (
	payloadLen = 1
	sizeLen    = 4
	headerLen  = payloadLen + sizeLen
)

type payloadFormat uint8

const (
	compressionNone payloadFormat = 0 // no compression
	compressionMade payloadFormat = 1 // compressed
)

// callInfo结构体包含了一个rpc所需要的所有的配置信息
type callInfo struct {
	compressorType        string
	failFast              bool
	maxReceiveMessageSize *int
	maxSendMessageSize    *int
	creds                 credentials.PerRPCCredentials
	contentSubtype        string
	codec                 encoding.Codec
	maxRetryRPCBufferSize int
}

func defaultCallInfo() *callInfo {
	return &callInfo{
		failFast:              true,
		maxRetryRPCBufferSize: 256 * 1024, // 256KB
	}
}

// CallOption configures a Call before it starts or extracts information from
// a Call after it completes.
type CallOption interface {
	// before is called before the call is sent to any server.  If before
	// returns a non-nil error, the RPC fails with that error.
	before(*callInfo) error

	// after is called after the call has completed.  after cannot return an
	// error, so any failures should be reported via output parameters.
	after(*callInfo, *csAttempt)
}

// EmptyCallOption does not alter the Call configuration.
// It can be embedded in another structure to carry satellite data for use
// by interceptors.
type EmptyCallOption struct{}

func (EmptyCallOption) before(*callInfo) error      { return nil }
func (EmptyCallOption) after(*callInfo, *csAttempt) {}

// Header returns a CallOptions that retrieves the header metadata
// for a unary RPC.
func Header(md *metadata.MD) CallOption {
	return HeaderCallOption{HeaderAddr: md}
}

// HeaderCallOption is a CallOption for collecting response header metadata.
// The metadata field will be populated *after* the RPC completes.
// This is an EXPERIMENTAL API.
type HeaderCallOption struct {
	HeaderAddr *metadata.MD
}

func (o HeaderCallOption) before(c *callInfo) error { return nil }
func (o HeaderCallOption) after(c *callInfo, attempt *csAttempt) {
	*o.HeaderAddr, _ = attempt.s.Header()
}

// Trailer returns a CallOptions that retrieves the trailer metadata
// for a unary RPC.
func Trailer(md *metadata.MD) CallOption {
	return TrailerCallOption{TrailerAddr: md}
}

// TrailerCallOption is a CallOption for collecting response trailer metadata.
// The metadata field will be populated *after* the RPC completes.
// This is an EXPERIMENTAL API.
type TrailerCallOption struct {
	TrailerAddr *metadata.MD
}

func (o TrailerCallOption) before(c *callInfo) error { return nil }
func (o TrailerCallOption) after(c *callInfo, attempt *csAttempt) {
	*o.TrailerAddr = attempt.s.Trailer()
}

// Peer returns a CallOption that retrieves peer information for a unary RPC.
// The peer field will be populated *after* the RPC completes.
func Peer(p *peer.Peer) CallOption {
	return PeerCallOption{PeerAddr: p}
}

// PeerCallOption is a CallOption for collecting the identity of the remote
// peer. The peer field will be populated *after* the RPC completes.
// This is an EXPERIMENTAL API.
type PeerCallOption struct {
	PeerAddr *peer.Peer
}

func (o PeerCallOption) before(c *callInfo) error { return nil }
func (o PeerCallOption) after(c *callInfo, attempt *csAttempt) {
	if x, ok := peer.FromContext(attempt.s.Context()); ok {
		*o.PeerAddr = *x
	}
}

// WaitForReady configures the action to take when an RPC is attempted on broken
// connections or unreachable servers. If waitForReady is false, the RPC will fail
// immediately. Otherwise, the RPC client will block the call until a
// connection is available (or the call is canceled or times out) and will
// retry the call if it fails due to a transient error.  gRPC will not retry if
// data was written to the wire unless the server indicates it did not process
// the data.  Please refer to
// https://github.com/grpc/grpc/blob/master/doc/wait-for-ready.md.
//
// By default, RPCs don't "wait for ready".
func WaitForReady(waitForReady bool) CallOption {
	return FailFastCallOption{FailFast: !waitForReady}
}

// FailFast is the opposite of WaitForReady.
//
// Deprecated: use WaitForReady.
func FailFast(failFast bool) CallOption {
	return FailFastCallOption{FailFast: failFast}
}

// FailFastCallOption is a CallOption for indicating whether an RPC should fail
// fast or not.
// This is an EXPERIMENTAL API.
type FailFastCallOption struct {
	FailFast bool
}

func (o FailFastCallOption) before(c *callInfo) error {
	c.failFast = o.FailFast
	return nil
}
func (o FailFastCallOption) after(c *callInfo, attempt *csAttempt) {}

// MaxCallRecvMsgSize returns a CallOption which sets the maximum message size
// in bytes the client can receive.
func MaxCallRecvMsgSize(bytes int) CallOption {
	return MaxRecvMsgSizeCallOption{MaxRecvMsgSize: bytes}
}

// MaxRecvMsgSizeCallOption is a CallOption that indicates the maximum message
// size in bytes the client can receive.
// This is an EXPERIMENTAL API.
type MaxRecvMsgSizeCallOption struct {
	MaxRecvMsgSize int
}

func (o MaxRecvMsgSizeCallOption) before(c *callInfo) error {
	c.maxReceiveMessageSize = &o.MaxRecvMsgSize
	return nil
}
func (o MaxRecvMsgSizeCallOption) after(c *callInfo, attempt *csAttempt) {}

// MaxCallSendMsgSize returns a CallOption which sets the maximum message size
// in bytes the client can send.
func MaxCallSendMsgSize(bytes int) CallOption {
	return MaxSendMsgSizeCallOption{MaxSendMsgSize: bytes}
}

// MaxSendMsgSizeCallOption is a CallOption that indicates the maximum message
// size in bytes the client can send.
// This is an EXPERIMENTAL API.
type MaxSendMsgSizeCallOption struct {
	MaxSendMsgSize int
}

func (o MaxSendMsgSizeCallOption) before(c *callInfo) error {
	c.maxSendMessageSize = &o.MaxSendMsgSize
	return nil
}
func (o MaxSendMsgSizeCallOption) after(c *callInfo, attempt *csAttempt) {}

// PerRPCCredentials returns a CallOption that sets credentials.PerRPCCredentials
// for a call.
func PerRPCCredentials(creds credentials.PerRPCCredentials) CallOption {
	return PerRPCCredsCallOption{Creds: creds}
}

// PerRPCCredsCallOption is a CallOption that indicates the per-RPC
// credentials to use for the call.
// This is an EXPERIMENTAL API.
type PerRPCCredsCallOption struct {
	Creds credentials.PerRPCCredentials
}

func (o PerRPCCredsCallOption) before(c *callInfo) error {
	c.creds = o.Creds
	return nil
}
func (o PerRPCCredsCallOption) after(c *callInfo, attempt *csAttempt) {}

// UseCompressor returns a CallOption which sets the compressor used when
// sending the request.  If WithCompressor is also set, UseCompressor has
// higher priority.
//
// This API is EXPERIMENTAL.
func UseCompressor(name string) CallOption {
	return CompressorCallOption{CompressorType: name}
}

// CompressorCallOption is a CallOption that indicates the compressor to use.
// This is an EXPERIMENTAL API.
type CompressorCallOption struct {
	CompressorType string
}

func (o CompressorCallOption) before(c *callInfo) error {
	c.compressorType = o.CompressorType
	return nil
}
func (o CompressorCallOption) after(c *callInfo, attempt *csAttempt) {}

// CallContentSubtype returns a CallOption that will set the content-subtype
// for a call. For example, if content-subtype is "json", the Content-Type over
// the wire will be "application/grpc+json". The content-subtype is converted
// to lowercase before being included in Content-Type. See Content-Type on
// https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#requests for
// more details.
//
// If ForceCodec is not also used, the content-subtype will be used to look up
// the Codec to use in the registry controlled by RegisterCodec. See the
// documentation on RegisterCodec for details on registration. The lookup of
// content-subtype is case-insensitive. If no such Codec is found, the call
// will result in an error with code codes.Internal.
//
// If ForceCodec is also used, that Codec will be used for all request and
// response messages, with the content-subtype set to the given contentSubtype
// here for requests.
func CallContentSubtype(contentSubtype string) CallOption {
	return ContentSubtypeCallOption{ContentSubtype: strings.ToLower(contentSubtype)}
}

// ContentSubtypeCallOption is a CallOption that indicates the content-subtype
// used for marshaling messages.
// This is an EXPERIMENTAL API.
type ContentSubtypeCallOption struct {
	ContentSubtype string
}

func (o ContentSubtypeCallOption) before(c *callInfo) error {
	c.contentSubtype = o.ContentSubtype
	return nil
}
func (o ContentSubtypeCallOption) after(c *callInfo, attempt *csAttempt) {}

// ForceCodec returns a CallOption that will set the given Codec to be
// used for all request and response messages for a call. The result of calling
// String() will be used as the content-subtype in a case-insensitive manner.
//
// See Content-Type on
// https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#requests for
// more details. Also see the documentation on RegisterCodec and
// CallContentSubtype for more details on the interaction between Codec and
// content-subtype.
//
// This function is provided for advanced users; prefer to use only
// CallContentSubtype to select a registered codec instead.
//
// This is an EXPERIMENTAL API.
func ForceCodec(codec encoding.Codec) CallOption {
	return ForceCodecCallOption{Codec: codec}
}

// ForceCodecCallOption is a CallOption that indicates the codec used for
// marshaling messages.
//
// This is an EXPERIMENTAL API.
type ForceCodecCallOption struct {
	Codec encoding.Codec
}

func (o ForceCodecCallOption) before(c *callInfo) error {
	c.codec = o.Codec
	return nil
}
func (o ForceCodecCallOption) after(c *callInfo, attempt *csAttempt) {}

// CallCustomCodec behaves like ForceCodec, but accepts a grpc.Codec instead of
// an encoding.Codec.
//
// Deprecated: use ForceCodec instead.
func CallCustomCodec(codec encoding.Codec) CallOption {
	return CustomCodecCallOption{Codec: codec}
}

// CustomCodecCallOption is a CallOption that indicates the codec used for
// marshaling messages.
//
// This is an EXPERIMENTAL API.
type CustomCodecCallOption struct {
	Codec encoding.Codec
}

func (o CustomCodecCallOption) before(c *callInfo) error {
	c.codec = o.Codec
	return nil
}
func (o CustomCodecCallOption) after(c *callInfo, attempt *csAttempt) {}

// MaxRetryRPCBufferSize returns a CallOption that limits the amount of memory
// used for buffering this RPC's requests for retry purposes.
//
// This API is EXPERIMENTAL.
func MaxRetryRPCBufferSize(bytes int) CallOption {
	return MaxRetryRPCBufferSizeCallOption{bytes}
}

// MaxRetryRPCBufferSizeCallOption is a CallOption indicating the amount of
// memory to be used for caching this RPC for retry purposes.
// This is an EXPERIMENTAL API.
type MaxRetryRPCBufferSizeCallOption struct {
	MaxRetryRPCBufferSize int
}

func (o MaxRetryRPCBufferSizeCallOption) before(c *callInfo) error {
	c.maxRetryRPCBufferSize = o.MaxRetryRPCBufferSize
	return nil
}
func (o MaxRetryRPCBufferSizeCallOption) after(c *callInfo, attempt *csAttempt) {}

//------------------------------解压、序列化、读取消息相关------------------------------
// 接受一个msg并解压
func recvAndDecompress(p *parser, s *transport.Stream,
	maxReceiveMessageSize int, compressor encoding.Compressor) ([]byte, error) {
	pf, d, err := p.recvMsg(maxReceiveMessageSize)
	if err != nil {
		return nil, err
	}
	if st := checkRecvPayload(pf, s.RecvCompress(), compressor != nil); st != nil {
		return nil, st.Err()
	}
	var size int
	if pf == compressionMade {
		d, size, err = decompress(compressor, d, maxReceiveMessageSize)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "grpc: failed to decompress the received message %v", err)
		}
	} else {
		size = len(d)
	}
	if size > maxReceiveMessageSize {
		return nil, status.Errorf(codes.ResourceExhausted, "grpc: received message larger than max (%d vs. %d)", size, maxReceiveMessageSize)
	}
	return d, nil
}

// 序列化一个消息
func encode(c encoding.Codec, msg interface{}) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	b, err := c.Marshal(msg)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "grpc: error while marshaling: %v", err.Error())
	}
	if uint(len(b)) > math.MaxUint32 {
		return nil, status.Errorf(codes.ResourceExhausted, "grpc: message too large (%d bytes)", len(b))
	}
	return b, nil
}

// 压缩一个消息
func compress(in []byte, compressor encoding.Compressor) ([]byte, error) {
	if compressor == nil {
		return nil, nil
	}
	wrapErr := func(err error) error {
		return status.Errorf(codes.Internal, "grpc: error while compressing: %v", err.Error())
	}
	buf := &bytes.Buffer{}
	z, err := compressor.Compress(buf)
	if err != nil {
		return nil, wrapErr(err)
	}
	if _, err := z.Write(in); err != nil {
		return nil, wrapErr(err)
	}
	if err := z.Close(); err != nil {
		return nil, wrapErr(err)
	}
	return buf.Bytes(), nil
}

// 返回一个消息头部(是否压缩+消息size) + data本身
func msgHeader(data, compData []byte) (hdr []byte, payload []byte) {
	hdr = make([]byte, headerLen)
	if compData != nil {
		hdr[0] = byte(compressionMade)
		data = compData
	} else {
		hdr[0] = byte(compressionNone)
	}
	binary.BigEndian.PutUint32(hdr[payloadLen:], uint32(len(data)))
	return hdr, data
}

// 校验压缩和压缩器是否一致
func checkRecvPayload(pf payloadFormat, recvCompress string, haveCompressor bool) *status.Status {
	switch pf {
	case compressionNone:
	case compressionMade:
		if recvCompress == "" || recvCompress == encoding.Identity {
			return status.New(codes.Internal, "grpc: compressed flag set with identity or empty encoding")
		}
		if !haveCompressor {
			return status.Newf(codes.Unimplemented, "grpc: Decompressor is not installed for grpc-encoding %q", recvCompress)
		}
	default:
		return status.Newf(codes.Internal, "grpc: received unexpected payload format %d", pf)
	}
	return nil
}

/**
 * @desc: 解压缩一个消息

 * @param:

 * @return []byte 解压缩之后的内容
		   int    解压缩之后的数据长度
*/
func decompress(compressor encoding.Compressor, d []byte, maxReceiveMessageSize int) ([]byte, int, error) {
	dcReader, err := compressor.Decompress(bytes.NewReader(d))
	if err != nil {
		return nil, 0, err
	}
	if sizer, ok := compressor.(interface {
		DecompressedSize(compressedBytes []byte) int
	}); ok {
		if size := sizer.DecompressedSize(d); size >= 0 {
			if size > maxReceiveMessageSize {
				return nil, size, nil
			}
			// size is used as an estimate to size the buffer, but we
			// will read more data if available.
			// +MinRead so ReadFrom will not reallocate if size is correct.
			buf := bytes.NewBuffer(make([]byte, 0, size+bytes.MinRead))
			bytesRead, err := buf.ReadFrom(io.LimitReader(dcReader, int64(maxReceiveMessageSize)+1))
			return buf.Bytes(), int(bytesRead), err
		}
	}
	// Read from LimitReader with limit max+1. So if the underlying
	// reader is over limit, the result will be bigger than max.
	d, err = ioutil.ReadAll(io.LimitReader(dcReader, int64(maxReceiveMessageSize)+1))
	return d, len(d), err
}

// 读取一个消息，并且反序列化到m
func recv(p *parser, c encoding.Codec, s *transport.Stream, m interface{}, maxReceiveMessageSize int, compressor encoding.Compressor) error {
	d, err := recvAndDecompress(p, s, maxReceiveMessageSize, compressor)
	if err != nil {
		return err
	}
	if err := c.Unmarshal(d, m); err != nil {
		return status.Errorf(codes.Internal, "grpc: failed to unmarshal the received message %v", err)
	}
	return nil
}

type rpcInfo struct {
	failfast      bool
	preloaderInfo *compressorInfo
}

type compressorInfo struct {
	codec encoding.Codec
	comp  encoding.Compressor
}

type rpcInfoContextKey struct{}

// 客户端会用
func newContextWithRPCInfo(ctx context.Context, failfast bool, codec encoding.Codec, comp encoding.Compressor) context.Context {
	return context.WithValue(ctx, rpcInfoContextKey{}, &rpcInfo{
		failfast: failfast,
		preloaderInfo: &compressorInfo{
			codec: codec,
			comp:  comp,
		},
	})
}

func rpcInfoFromContext(ctx context.Context) (s *rpcInfo, ok bool) {
	s, ok = ctx.Value(rpcInfoContextKey{}).(*rpcInfo)
	return
}

// Deprecated: use status.Code instead.
func Code(err error) codes.Code {
	return status.Code(err)
}

// Deprecated: use status.Convert and Message method instead.
func ErrorDesc(err error) string {
	return status.Convert(err).Message()
}

// Deprecated: use status.Errorf instead.
func Errorf(c codes.Code, format string, a ...interface{}) error {
	return status.Errorf(c, format, a...)
}

func toRPCErr(err error) error {
	if err == nil || err == io.EOF {
		return err
	}
	if err == io.ErrUnexpectedEOF {
		return status.Error(codes.Internal, err.Error())
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch e := err.(type) {
	case transport.ConnectionError:
		return status.Error(codes.Unavailable, e.Desc)
	default:
		switch err {
		case context.DeadlineExceeded:
			return status.Error(codes.DeadlineExceeded, err.Error())
		case context.Canceled:
			return status.Error(codes.Canceled, err.Error())
		}
	}
	return status.Error(codes.Unknown, err.Error())
}

func setCallInfoCodec(c *callInfo) error {
	if c.codec != nil {
		// codec was already set by a CallOption; use it.
		return nil
	}
	if c.contentSubtype == "" {
		// No codec specified in CallOptions; use proto by default.
		c.codec = encoding.GetCodec(proto.Name)
		return nil
	}
	// c.contentSubtype is already lowercased in CallContentSubtype
	c.codec = encoding.GetCodec(c.contentSubtype)
	if c.codec == nil {
		return status.Errorf(codes.Internal, "no codec registered for content-subtype %s", c.contentSubtype)
	}
	return nil
}

func parseDialTarget(target string) (net string, addr string) {
	net = "tcp"
	m1 := strings.Index(target, ":")
	m2 := strings.Index(target, ":/")
	if m1 >= 0 && m2 < 0 {
		if n := target[0:m1]; n == "unix" {
			net = n
			addr = target[m1+1:]
			return net, addr
		}
	}
	if m2 >= 0 {
		t, err := url.Parse(target)
		if err != nil {
			return net, target
		}
		scheme := t.Scheme
		addr = t.Path
		if scheme == "unix" {
			net = scheme
			if addr == "" {
				addr = t.Host
			}
			return net, addr
		}
	}
	return net, target
}

type parser struct {
	r      io.Reader // 一般赋值为stream
	header [5]byte   // grpc消息头
}

/**
 * @desc: 从recBuffer读取给定长度的内容

 * @param:

 * @return pf 表示是否此内容是压缩过的
           msg 消息体
		   err 异常
*/
// If there is an error, possible values are:
//   * io.EOF, when no messages remain
//   * io.ErrUnexpectedEOF
//   * of type transport.ConnectionError
//   * an error from the status package
// No other error values or types must be returned, which also means
// that the underlying io.Reader must not return an incompatible
// error.
func (p *parser) recvMsg(maxReceiveMessageSize int) (pf payloadFormat, msg []byte, err error) {
	// 读取头信息
	if _, err := p.r.Read(p.header[:]); err != nil {
		return 0, nil, err
	}
	// 获取是否被压缩过的flag
	pf = payloadFormat(p.header[0])
	// 获取消息体长度
	length := binary.BigEndian.Uint32(p.header[1:])
	if length == 0 {
		return pf, nil, nil
	}
	if int64(length) > int64(maxInt) {
		return 0, nil, status.Errorf(codes.ResourceExhausted, "grpc: received message larger than max length allowed on current machine (%d vs. %d)", length, maxInt)
	}
	if int(length) > maxReceiveMessageSize {
		return 0, nil, status.Errorf(codes.ResourceExhausted, "grpc: received message larger than max (%d vs. %d)", length, maxReceiveMessageSize)
	}
	// 获取指定长度的内容信息
	msg = make([]byte, int(length))
	if _, err := p.r.Read(msg); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	return pf, msg, nil
}
