package sanctuary

import (
	"errors"
	"io"
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

type slot struct {
	block []byte
	len   int
}

type ringBuffer struct {
	slots []slot
	head  int
	tail  int
	count int
	mu    sync.Mutex
}

func newRingBuffer(capacity int) *ringBuffer {
	return &ringBuffer{
		slots: make([]slot, capacity),
	}
}

func (rb *ringBuffer) push(block []byte, dataLen int) bool {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.count == len(rb.slots) {
		return false
	}
	rb.slots[rb.tail] = slot{block: block, len: dataLen}
	rb.tail = (rb.tail + 1) % len(rb.slots)
	rb.count++
	return true
}

func (rb *ringBuffer) pop() ([]byte, int, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	if rb.count == 0 {
		return nil, 0, false
	}
	s := rb.slots[rb.head]
	rb.slots[rb.head] = slot{}
	rb.head = (rb.head + 1) % len(rb.slots)
	rb.count--
	return s.block, s.len, true
}

func (rb *ringBuffer) len() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return rb.count
}

func (rb *ringBuffer) cap() int {
	return len(rb.slots)
}

func (rb *ringBuffer) free() int {
	return rb.cap() - rb.len()
}

func (rb *ringBuffer) reset() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	for i := 0; i < rb.count; i++ {
		idx := (rb.head + i) % len(rb.slots)
		putBlock(rb.slots[idx].block)
		rb.slots[idx] = slot{}
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

	if !s.buf.push(block, n) {
		putBlock(block)
		return ErrBufferFull
	}
	return nil
}

func (s *Sanctuary) Pop() ([]byte, bool) {
	block, dataLen, ok := s.buf.pop()
	if !ok {
		return nil, false
	}
	defer putBlock(block)
	out := make([]byte, dataLen)
	copy(out, block[:dataLen])
	return out, true
}

func (s *Sanctuary) PopTo(w io.Writer) (int, error, bool) {
	block, dataLen, ok := s.buf.pop()
	if !ok {
		return 0, nil, false
	}
	defer putBlock(block)
	n, err := w.Write(block[:dataLen])
	return n, err, true
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
	block, dataLen, ok := s.buf.pop()
	if !ok {
		return nil, false
	}
	out := make([]byte, dataLen)
	copy(out, block[:dataLen])
	putBlock(block)
	return out, true
}
