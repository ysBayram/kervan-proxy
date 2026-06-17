package pipeline

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ysBayram/kervan-proxy/internal/interceptor"
	"github.com/ysBayram/kervan-proxy/internal/valve"
)

func TestPipelineOpenDirectFlow(t *testing.T) {
	srcR, srcW := io.Pipe()
	var dst safeBuffer

	p := NewPipeline(srcR, &dst)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}

	_, err := srcW.Write([]byte("hello, world"))
	if err != nil {
		t.Fatal(err)
	}
	srcW.Close()
	time.Sleep(100 * time.Millisecond)

	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
	if dst.String() != "hello, world" {
		t.Fatalf("expected 'hello, world', got %q", dst.String())
	}
}

func TestPipelineHeldBuffersData(t *testing.T) {
	srcR, srcW := io.Pipe()
	var dst safeBuffer

	p := NewPipeline(srcR, &dst)
	p.Start()
	defer p.Stop()

	if err := p.Valve().TransitionTo(valve.HELD); err != nil {
		t.Fatal(err)
	}

	srcW.Write([]byte("data1"))
	time.Sleep(50 * time.Millisecond)
	srcW.Write([]byte("data2"))
	time.Sleep(50 * time.Millisecond)
	srcW.Close()
	time.Sleep(100 * time.Millisecond)

	if p.Sanctuary().Len() != 2 {
		t.Fatalf("expected 2 items in sanctuary, got %d", p.Sanctuary().Len())
	}
	if dst.Len() != 0 {
		t.Fatalf("expected 0 bytes in target while HELD, got %d", dst.Len())
	}
}

func TestPipelineFullCycle(t *testing.T) {
	srcR, srcW := io.Pipe()
	var dst safeBuffer

	p := NewPipeline(srcR, &dst)
	p.Start()
	defer p.Stop()

	if err := p.Valve().TransitionTo(valve.HELD); err != nil {
		t.Fatal(err)
	}

	srcW.Write([]byte("buf1"))
	time.Sleep(50 * time.Millisecond)
	srcW.Write([]byte("buf2"))
	time.Sleep(50 * time.Millisecond)
	srcW.Write([]byte("buf3"))
	time.Sleep(50 * time.Millisecond)
	srcW.Close()

	if p.Sanctuary().Len() != 3 {
		t.Fatalf("expected 3 buffered items after HELD, got %d", p.Sanctuary().Len())
	}
	if dst.Len() != 0 {
		t.Fatalf("expected 0 bytes in target while HELD, got %d", dst.Len())
	}

	if err := p.Valve().TransitionTo(valve.DRAINING); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	if p.Sanctuary().Len() != 0 {
		t.Fatalf("expected sanctuary empty after drain, got %d", p.Sanctuary().Len())
	}
	if dst.String() != "buf1buf2buf3" {
		t.Fatalf("expected all buffered data in target, got %q", dst.String())
	}
	if p.Valve().State() != valve.OPEN {
		t.Fatalf("expected valve OPEN after drain, got %s", p.Valve().State())
	}
}

func TestPipelineCascadingFailure(t *testing.T) {
	srcR, srcW := io.Pipe()
	failTarget := &failWriter{failAfter: 0}

	p := NewPipeline(srcR, failTarget)
	p.Start()
	defer p.Stop()

	if err := p.Valve().TransitionTo(valve.HELD); err != nil {
		t.Fatal(err)
	}

	srcW.Write([]byte("a"))
	time.Sleep(50 * time.Millisecond)
	srcW.Write([]byte("b"))
	time.Sleep(50 * time.Millisecond)
	srcW.Write([]byte("c"))
	time.Sleep(50 * time.Millisecond)
	srcW.Close()

	if p.Sanctuary().Len() != 3 {
		t.Fatalf("expected 3 items in sanctuary, got %d", p.Sanctuary().Len())
	}

	if err := p.Valve().TransitionTo(valve.DRAINING); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	if p.Valve().State() != valve.HELD {
		t.Fatalf("expected valve HELD after cascading failure, got %s", p.Valve().State())
	}
	if p.Sanctuary().Len() == 0 {
		t.Fatal("expected sanctuary to have preserved items after cascading failure")
	}
}

func TestPipelineStartStopIdempotent(t *testing.T) {
	srcR, srcW := io.Pipe()
	var dst safeBuffer
	p := NewPipeline(srcR, &dst)
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	srcW.Close()
	time.Sleep(50 * time.Millisecond)
	if err := p.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestPipelineInterceptorTriggers(t *testing.T) {
	srcR, srcW := io.Pipe()
	var dst safeBuffer

	p := NewPipeline(srcR, &dst)
	var fired int32
	p.RegisterRule(interceptor.Rule{
		Name: "test-rule",
		Predicate: func() bool {
			return true
		},
		Action: func() {
			atomic.AddInt32(&fired, 1)
		},
	})
	p.Start()
	defer p.Stop()

	srcW.Write([]byte("ping"))
	srcW.Close()
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&fired) == 0 {
		t.Fatal("expected interceptor to fire")
	}
}

type safeBuffer struct {
	mu  sync.Mutex
	buf []byte
}

func (sb *safeBuffer) Write(p []byte) (int, error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.buf = append(sb.buf, p...)
	return len(p), nil
}

func (sb *safeBuffer) String() string {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return string(sb.buf)
}

func (sb *safeBuffer) Len() int {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return len(sb.buf)
}

type failWriter struct {
	mu        sync.Mutex
	buf       []byte
	failAfter int
	written   int
}

func (fw *failWriter) Write(p []byte) (int, error) {
	fw.mu.Lock()
	defer fw.mu.Unlock()
	fw.written++
	if fw.written > fw.failAfter {
		return 0, &writeError{"simulated target failure"}
	}
	fw.buf = append(fw.buf, p...)
	return len(p), nil
}

type writeError struct{ s string }

func (e *writeError) Error() string { return e.s }
