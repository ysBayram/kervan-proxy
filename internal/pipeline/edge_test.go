package pipeline

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ysBayram/kervan-proxy/internal/sanctuary"
	"github.com/ysBayram/kervan-proxy/internal/valve"
)

func TestTargetPoisoningPreservesData(t *testing.T) {
	srcR, srcW := io.Pipe()
	failTarget := &failWriter{failAfter: 0}

	p := NewPipeline(srcR, failTarget)
	p.Start()
	defer p.Stop(0)

	srcW.Write([]byte("preserve-me"))
	time.Sleep(100 * time.Millisecond)
	srcW.Close()

	if p.Valve().State() != valve.HELD {
		t.Fatalf("expected valve HELD after target poisoning, got %s", p.Valve().State())
	}
	if p.Sanctuary().Len() != 1 {
		t.Fatalf("expected 1 preserved item in sanctuary, got %d", p.Sanctuary().Len())
	}
}

func TestBackpressureDropOldestOnOverflow(t *testing.T) {
	srcR, srcW := io.Pipe()
	var dst safeBuffer

	p := NewPipeline(srcR, &dst, func(pl *Pipeline) {
		pl.cfg.SanctuaryCapacity = 2
		pl.cfg.BackpressureAction = sanctuary.DropOldest
		pl.sanctuary = sanctuary.NewSanctuary(2)
	})
	p.Start()
	defer p.Stop(0)

	p.Valve().TransitionTo(valve.HELD)

	srcW.Write([]byte("first"))
	time.Sleep(50 * time.Millisecond)
	srcW.Write([]byte("second"))
	time.Sleep(50 * time.Millisecond)
	srcW.Write([]byte("third"))
	time.Sleep(50 * time.Millisecond)
	srcW.Close()

	time.Sleep(100 * time.Millisecond)

	if p.Sanctuary().Len() != 2 {
		t.Fatalf("expected 2 items after drop, got %d", p.Sanctuary().Len())
	}

	p.Valve().TransitionTo(valve.DRAINING)
	time.Sleep(200 * time.Millisecond)

	got := dst.String()
	if got != "secondthird" {
		t.Fatalf("expected 'secondthird' after drain, got %q", got)
	}
}

func TestBackpressureRejectNewOnOverflow(t *testing.T) {
	srcR, srcW := io.Pipe()
	var dst safeBuffer

	p := NewPipeline(srcR, &dst, func(pl *Pipeline) {
		pl.cfg.SanctuaryCapacity = 2
		pl.cfg.BackpressureAction = sanctuary.RejectNew
		pl.sanctuary = sanctuary.NewSanctuary(2)
	})
	p.Start()
	defer p.Stop(0)

	p.Valve().TransitionTo(valve.HELD)

	srcW.Write([]byte("keep1"))
	time.Sleep(50 * time.Millisecond)
	srcW.Write([]byte("keep2"))
	time.Sleep(50 * time.Millisecond)
	srcW.Write([]byte("rejected"))
	time.Sleep(50 * time.Millisecond)
	srcW.Close()

	time.Sleep(100 * time.Millisecond)

	if p.Sanctuary().Len() != 2 {
		t.Fatalf("expected 2 items preserved after reject, got %d", p.Sanctuary().Len())
	}

	p.Valve().TransitionTo(valve.DRAINING)
	time.Sleep(200 * time.Millisecond)

	got := dst.String()
	if got != "keep1keep2" {
		t.Fatalf("expected 'keep1keep2' after drain, got %q", got)
	}
}

func TestCascadingFailureDuringDrain(t *testing.T) {
	srcR, srcW := io.Pipe()
	failTarget := &failWriter{failAfter: 1}

	p := NewPipeline(srcR, failTarget)
	p.Start()
	defer p.Stop(0)

	p.Valve().TransitionTo(valve.HELD)

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
		t.Fatal("expected sanctuary to have remaining items after cascading failure")
	}
}

func TestZombieSourceCleanup(t *testing.T) {
	srcR, srcW := io.Pipe()
	var dst safeBuffer

	p := NewPipeline(srcR, &dst)
	p.Start()
	defer p.Stop(0)

	p.Valve().TransitionTo(valve.HELD)

	srcW.Write([]byte("orphaned"))
	srcW.Close()
	time.Sleep(100 * time.Millisecond)

	if !p.sourceDone.Load() {
		t.Fatal("expected sourceDone after source EOF")
	}
	if p.Sanctuary().Len() != 1 {
		t.Fatalf("expected 1 item in sanctuary, got %d", p.Sanctuary().Len())
	}

	p.Valve().TransitionTo(valve.DRAINING)
	time.Sleep(200 * time.Millisecond)

	if p.Sanctuary().Len() != 0 {
		t.Fatalf("expected sanctuary empty after drain, got %d", p.Sanctuary().Len())
	}
	if dst.String() != "orphaned" {
		t.Fatalf("expected 'orphaned' in target, got %q", dst.String())
	}
}

func TestGracefulShutdownDuringDrain(t *testing.T) {
	srcR, srcW := io.Pipe()
	var dst safeBuffer

	p := NewPipeline(srcR, &dst)
	p.Start()

	p.Valve().TransitionTo(valve.HELD)

	srcW.Write([]byte("data-to-drain"))
	time.Sleep(50 * time.Millisecond)
	srcW.Close()

	p.Valve().TransitionTo(valve.DRAINING)
	time.Sleep(50 * time.Millisecond)

	if err := p.Stop(0); err != nil {
		t.Fatal(err)
	}

	final := dst.String()
	if final != "data-to-drain" {
		t.Fatalf("expected 'data-to-drain' in target after shutdown, got %q", final)
	}
}

func TestConcurrentSourceAndTargetWrites(t *testing.T) {
	srcR, srcW := io.Pipe()
	var mu sync.Mutex
	var total int
	var writeCount int32

	p := NewPipeline(srcR, writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		total += len(p)
		mu.Unlock()
		atomic.AddInt32(&writeCount, 1)
		return len(p), nil
	}))
	p.Start()
	defer p.Stop(0)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			srcW.Write([]byte("concurrent-payload"))
		}()
	}
	wg.Wait()
	srcW.Close()
	time.Sleep(200 * time.Millisecond)

	expected := 10 * len("concurrent-payload")
	mu.Lock()
	got := total
	mu.Unlock()

	if got != expected {
		t.Fatalf("expected %d bytes, got %d", expected, got)
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
