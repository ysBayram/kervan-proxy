package monitoring

import "context"

type Recorder interface {
	Start(ctx context.Context) error
	Stop() error
}
