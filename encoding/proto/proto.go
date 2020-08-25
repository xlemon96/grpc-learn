package proto

import (
	"math"
	"sync"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/encoding"
)

const Name = "proto"

func init() {
	encoding.RegisterCodec(codec{})
}

type codec struct{}

type cachedProtoBuffer struct {
	lastMarshaledSize uint32
	proto.Buffer
}

func capToMaxInt32(val int) uint32 {
	if val > math.MaxInt32 {
		return uint32(math.MaxInt32)
	}
	return uint32(val)
}

func marshal(v interface{}, cb *cachedProtoBuffer) ([]byte, error) {
	protoMsg := v.(proto.Message)
	newSlice := make([]byte, 0, cb.lastMarshaledSize)

	cb.SetBuf(newSlice)
	cb.Reset()
	if err := cb.Marshal(protoMsg); err != nil {
		return nil, err
	}
	out := cb.Bytes()
	cb.lastMarshaledSize = capToMaxInt32(len(out))
	return out, nil
}

func (codec) Marshal(v interface{}) ([]byte, error) {
	if pm, ok := v.(proto.Marshaler); ok {
		// object can marshal itself, no need for buffer
		return pm.Marshal()
	}

	cb := protoBufferPool.Get().(*cachedProtoBuffer)
	out, err := marshal(v, cb)

	// put back buffer and lose the ref to the slice
	cb.SetBuf(nil)
	protoBufferPool.Put(cb)
	return out, err
}

func (codec) Unmarshal(data []byte, v interface{}) error {
	protoMsg := v.(proto.Message)
	protoMsg.Reset()

	if pu, ok := protoMsg.(proto.Unmarshaler); ok {
		// object can unmarshal itself, no need for buffer
		return pu.Unmarshal(data)
	}

	cb := protoBufferPool.Get().(*cachedProtoBuffer)
	cb.SetBuf(data)
	err := cb.Unmarshal(protoMsg)
	cb.SetBuf(nil)
	protoBufferPool.Put(cb)
	return err
}

func (codec) Name() string {
	return Name
}

var protoBufferPool = &sync.Pool{
	New: func() interface{} {
		return &cachedProtoBuffer{
			Buffer:            proto.Buffer{},
			lastMarshaledSize: 16,
		}
	},
}
