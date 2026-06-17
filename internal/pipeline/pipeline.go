package pipeline

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/ysBayram/kervan-proxy/internal/interceptor"
	"github.com/ysBayram/kervan-proxy/internal/sanctuary"
	"github.com/ysBayram/kervan-proxy/internal/valve"
)

type Config struct {
	SanctuaryCapacity  int
	ReadBufferSize     int
	BackpressureAction sanctuary.BackpressureAction
}

type Monitor interface {
	Record(ctx context.Context, eventType string, metadata map[string]any)
}

type Pipeline struct {
	source     io.Reader
	target     io.Writer
	valve      *valve.Valve
	sanctuary  *sanctuary.Sanctuary
	intercepts *interceptor.InterceptorChain
	monitor    Monitor
	cfg        Config

	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	mu         sync.Mutex
	sourceDone atomic.Bool
}

type Option func(*Pipeline)

func WithMonitor(m Monitor) Option {
	return func(p *Pipeline) {
		p.monitor = m
	}
}

func NewPipeline(source io.Reader, target io.Writer, opts ...Option) *Pipeline {
	cfg := Config{
		SanctuaryCapacity: 10000,
		ReadBufferSize:    4096,
	}
	p := &Pipeline{
		source:     source,
		target:     target,
		valve:      valve.NewValve(valve.OPEN),
		sanctuary:  sanctuary.NewSanctuary(cfg.SanctuaryCapacity),
		intercepts: interceptor.NewInterceptorChain(),
		cfg:        cfg,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *Pipeline) RegisterRule(rule interceptor.Rule) {
	p.intercepts.AddRule(rule)
}

func (p *Pipeline) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx != nil {
		return errors.New("pipeline already started")
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.wg.Add(2)
	go p.ingestionLoop()
	go p.executionLoop()
	return nil
}

func (p *Pipeline) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancel != nil {
		p.cancel()
	}
	p.wg.Wait()
	return nil
}

func (p *Pipeline) Valve() *valve.Valve {
	return p.valve
}

func (p *Pipeline) Sanctuary() *sanctuary.Sanctuary {
	return p.sanctuary
}

func (p *Pipeline) InterceptorChain() *interceptor.InterceptorChain {
	return p.intercepts
}

func (p *Pipeline) ingestionLoop() {
	defer p.wg.Done()
	buf := make([]byte, p.cfg.ReadBufferSize)

	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		n, err := p.source.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				p.emitEvent("source_disconnect", map[string]any{"reason": "eof"})
			} else {
				p.emitEvent("source_disconnect", map[string]any{"reason": err.Error()})
			}
			p.sourceDone.Store(true)
			return
		}

		data := make([]byte, n)
		copy(data, buf[:n])

		st := p.valve.State()
		switch st {
		case valve.OPEN:
			if _, err := p.target.Write(data); err != nil {
				p.emitEvent("target_failure", map[string]any{"error": err.Error()})
				p.sanctuary.Push(data)
				p.valve.TransitionTo(valve.HELD)
				p.emitEvent("valve_transition", map[string]any{
					"from": "OPEN",
					"to":   "HELD",
				})
			}
		case valve.HELD, valve.DRAINING:
			if err := p.sanctuary.Push(data); err != nil {
				action := p.cfg.BackpressureAction
				if action == sanctuary.RejectNew {
					p.emitEvent("backpressure", map[string]any{
						"action": "reject_new",
						"len":    p.sanctuary.Len(),
						"cap":    p.sanctuary.Cap(),
						"reason": err.Error(),
					})
				} else {
					p.emitEvent("backpressure", map[string]any{
						"action": "drop_oldest",
						"len":    p.sanctuary.Len(),
						"cap":    p.sanctuary.Cap(),
						"reason": err.Error(),
					})
					p.sanctuary.DropOldest()
					p.sanctuary.Push(data)
				}
			}
		}
	}
}

func (p *Pipeline) executionLoop() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		st := p.valve.State()

		switch st {
		case valve.HELD:
			p.intercepts.Evaluate()
			continue

		case valve.DRAINING:
			p.intercepts.Evaluate()
			for {
				select {
				case <-p.ctx.Done():
					return
				default:
				}
				data, ok := p.sanctuary.Pop()
				if !ok {
					if err := p.valve.TransitionTo(valve.OPEN); err != nil {
						return
					}
					p.emitEvent("valve_transition", map[string]any{
						"from": "DRAINING",
						"to":   "OPEN",
					})
					break
				}
				if _, err := p.target.Write(data); err != nil {
					p.emitEvent("target_failure", map[string]any{"error": err.Error()})
					if err := p.valve.TransitionTo(valve.HELD); err != nil {
						return
					}
					p.emitEvent("valve_transition", map[string]any{
						"from": "DRAINING",
						"to":   "HELD",
					})
					break
				}
			}

		case valve.OPEN:
			if p.sourceDone.Load() && p.sanctuary.Len() == 0 {
				return
			}
			p.intercepts.Evaluate()
		}
	}
}

func (p *Pipeline) emitEvent(eventType string, metadata map[string]any) {
	if p.monitor != nil {
		p.monitor.Record(p.ctx, eventType, metadata)
	}
}
