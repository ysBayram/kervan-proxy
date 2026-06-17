package sanctuary

import (
	"sync"
	"testing"
)

func TestSanctuaryPushPop(t *testing.T) {
	s := NewSanctuary(3)
	if s.Len() != 0 {
		t.Fatalf("expected Len=0, got %d", s.Len())
	}
	if s.Cap() != 3 {
		t.Fatalf("expected Cap=3, got %d", s.Cap())
	}

	data := []byte("hello")
	if err := s.Push(data); err != nil {
		t.Fatal(err)
	}
	if s.Len() != 1 {
		t.Fatalf("expected Len=1, got %d", s.Len())
	}

	got, ok := s.Pop()
	if !ok {
		t.Fatal("expected pop to succeed")
	}
	if string(got) != "hello" {
		t.Fatalf("expected 'hello', got %q", string(got))
	}
	if s.Len() != 0 {
		t.Fatalf("expected Len=0 after pop, got %d", s.Len())
	}
}

func TestSanctuaryFIFOOrder(t *testing.T) {
	s := NewSanctuary(10)
	msgs := []string{"a", "b", "c", "d", "e"}
	for _, m := range msgs {
		if err := s.Push([]byte(m)); err != nil {
			t.Fatal(err)
		}
	}
	for _, expected := range msgs {
		got, ok := s.Pop()
		if !ok {
			t.Fatalf("expected %q, got nothing", expected)
		}
		if string(got) != expected {
			t.Fatalf("expected %q, got %q", expected, string(got))
		}
	}
}

func TestSanctuaryOverflow(t *testing.T) {
	s := NewSanctuary(2)
	if err := s.Push([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := s.Push([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if err := s.Push([]byte("c")); err != ErrBufferFull {
		t.Fatalf("expected ErrBufferFull, got %v", err)
	}
}

func TestSanctuaryEmptyPop(t *testing.T) {
	s := NewSanctuary(3)
	_, ok := s.Pop()
	if ok {
		t.Fatal("expected pop to fail on empty")
	}
}

func TestSanctuaryConcurrent(t *testing.T) {
	s := NewSanctuary(100)
	var wg sync.WaitGroup
	n := 50

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := s.Push([]byte{byte(i)}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()

	if s.Len() != n {
		t.Fatalf("expected Len=%d, got %d", n, s.Len())
	}

	seen := make(map[byte]bool)
	for i := 0; i < n; i++ {
		got, ok := s.Pop()
		if !ok {
			t.Fatal("expected pop to succeed")
		}
		if len(got) > 0 {
			seen[got[0]] = true
		}
	}
	if len(seen) != n {
		t.Fatalf("expected %d unique items, got %d", n, len(seen))
	}
}

func TestSanctuaryReset(t *testing.T) {
	s := NewSanctuary(5)
	for i := 0; i < 3; i++ {
		s.Push([]byte{byte(i)})
	}
	s.Reset()
	if s.Len() != 0 {
		t.Fatalf("expected Len=0 after reset, got %d", s.Len())
	}
	if s.Free() != s.Cap() {
		t.Fatalf("expected Free == Cap after reset")
	}
}

func TestSanctuaryDropOldest(t *testing.T) {
	s := NewSanctuary(2)
	s.Push([]byte("first"))
	s.Push([]byte("second"))

	dropped, ok := s.DropOldest()
	if !ok {
		t.Fatal("expected DropOldest to succeed")
	}
	if string(dropped) != "first" {
		t.Fatalf("expected 'first' dropped, got %q", string(dropped))
	}
	if s.Len() != 1 {
		t.Fatalf("expected Len=1 after drop, got %d", s.Len())
	}
}

func TestSanctuaryMemoryFootprint(t *testing.T) {
	s := NewSanctuary(10)
	if s.MemoryFootprint() != 0 {
		t.Fatalf("expected 0 footprint, got %d", s.MemoryFootprint())
	}
	s.Push([]byte("hello"))
	if s.MemoryFootprint() != int64(blockSize) {
		t.Fatalf("expected %d footprint, got %d", blockSize, s.MemoryFootprint())
	}
}
