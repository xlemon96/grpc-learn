package connectivity

import (
	"context"

	"google.golang.org/grpc/grpclog"
)

type State int

func (s State) String() string {
	switch s {
	case Idle:
		return "IDLE"
	case Connecting:
		return "CONNECTING"
	case Ready:
		return "READY"
	case TransientFailure:
		return "TRANSIENT_FAILURE"
	case Shutdown:
		return "SHUTDOWN"
	default:
		grpclog.Errorf("unknown connectivity state: %d", s)
		return "Invalid-State"
	}
}

const (
	Idle State = iota
	Connecting
	Ready
	TransientFailure // TransientFailure indicates the ClientConn has seen a failure but expects to recover.
	Shutdown
)

type Reporter interface {
	CurrentState() State
	WaitForStateChange(context.Context, State) bool
}
