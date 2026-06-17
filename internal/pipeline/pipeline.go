package pipeline

import (
	"github.com/ysBayram/kervan-proxy/internal/interceptor"
)

type Pipeline interface {
	Start() error
	Stop() error
	RegisterRule(rule interceptor.Rule)
}
