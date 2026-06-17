package sanctuary

import (
	"errors"
	"sync"
)

var (
	ErrBufferFull = errors.New("sanctuary buffer full")

	bytePool = sync.Pool{
		New: func() any {
			buf := make([]byte, blockSize)
			return &buf
		},
	}
	blockSize = 4096
)

type BackpressureAction int

const (
	DropOldest BackpressureAction = iota
	RejectNew
)

func (a BackpressureAction) String() string {
	switch a {
	case DropOldest:
		return "drop_oldest"
	case RejectNew:
		return "reject_new"
	default:
		return "unknown"
	}
}

func getBlock() []byte {
	return *bytePool.Get().(*[]byte)
}

func putBlock(buf []byte) {
	if cap(buf) != blockSize {
		return
	}
	clear(buf)
	bytePool.Put(&buf)
}

type ringBuffer struct {
	blocks [][]byte
	head   int
	tail   int
	count  int
	mu     sync.Mutex
}

func newRingBuffer(capacity int) *ringBuffer {
	return &ringBuffer{
		blocks: make([][]byte, capacity),
	}
}

func (rb *ringBuffer) push(block []byte) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.count == len(rb.blocks) {
		return false
	}
	rb.blocks[rb.tail] = block
	rb.tail = (rb.tail + 1) % len(rb.blocks)
	rb.count++
	return true
}

func (rb *ringBuffer) pop() ([]byte, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.count == 0 {
		return nil, false
	}
	block := rb.blocks[rb.head]
	rb.blocks[rb.head] = nil
	rb.head = (rb.head + 1) % len(rb.blocks)
	rb.count--
	return block, true
}

func (rb *ringBuffer) len() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.count
}

func (rb *ringBuffer) cap() int {
	return len(rb.blocks)
}

func (rb *ringBuffer) free() int {
	return rb.cap() - rb.len()
}

func (rb *ringBuffer) reset() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for i := 0; i < rb.count; i++ {
		idx := (rb.head + i) % len(rb.blocks)
		putBlock(rb.blocks[idx])
		rb.blocks[idx] = nil
	}
	rb.head = 0
	rb.tail = 0
	rb.count = 0
}

type Sanctuary struct {
	buf *ringBuffer
}

func NewSanctuary(capacity int) *Sanctuary {
	return &Sanctuary{
		buf: newRingBuffer(capacity),
	}
}

func (s *Sanctuary) Push(data []byte) error {
	block := getBlock()
	n := copy(block, data)
	_ = n

	if !s.buf.push(block) {
		putBlock(block)
		return ErrBufferFull
	}
	return nil
}

func (s *Sanctuary) Pop() ([]byte, bool) {
	block, ok := s.buf.pop()
	if !ok {
		return nil, false
	}
	defer putBlock(block)
	out := make([]byte, len(block))
	copy(out, block)
	return out, true
}

func (s *Sanctuary) Len() int {
	return s.buf.len()
}

func (s *Sanctuary) Cap() int {
	return s.buf.cap()
}

func (s *Sanctuary) Free() int {
	return s.buf.free()
}

func (s *Sanctuary) MemoryFootprint() int64 {
	return int64(s.Len()) * int64(blockSize)
}

func (s *Sanctuary) Reset() {
	s.buf.reset()
}

func (s *Sanctuary) DropOldest() ([]byte, bool) {
	block, ok := s.buf.pop()
	if !ok {
		return nil, false
	}
	putBlock(block)
	return block, true
}
