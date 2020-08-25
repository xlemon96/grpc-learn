package transport

import (
	"math"
	"time"
)

const (
	defaultWindowSize              = 65535             // 默认的窗口大小
	initialWindowSize              = defaultWindowSize // 初始化的窗口大小
	infinity                       = time.Duration(math.MaxInt64)
	defaultClientKeepaliveTime     = infinity
	defaultClientKeepaliveTimeout  = 20 * time.Second
	defaultMaxStreamsClient        = 100
	defaultMaxConnectionIdle       = infinity
	defaultMaxConnectionAge        = infinity
	defaultMaxConnectionAgeGrace   = infinity
	defaultServerKeepaliveTime     = 2 * time.Hour
	defaultServerKeepaliveTimeout  = 20 * time.Second
	defaultKeepalivePolicyMinTime  = 5 * time.Minute
	maxWindowSize                  = math.MaxInt32 // 最大的窗口大小
	defaultWriteQuota              = 64 * 1024     // 默认发送数据的配额
	defaultClientMaxHeaderListSize = uint32(16 << 20)
	defaultServerMaxHeaderListSize = uint32(16 << 20)
)
