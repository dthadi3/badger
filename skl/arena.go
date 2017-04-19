package skl

import (
	"sync/atomic"

	"github.com/dgraph-io/badger/y"
)

// Arena should be lock-free.
type Arena struct {
	n   uint32
	buf []byte
}

// NewArena returns a new arena.
func NewArena(n int64) *Arena {
	out := &Arena{
		buf: make([]byte, n),
	}
	return out
}

func (s *Arena) Size() int64 {
	return int64(atomic.LoadUint32(&s.n))
}

func (s *Arena) Reset() {
	atomic.StoreUint32(&s.n, 0)
}

// Put will *copy* val into arena. To make better use of this, reuse your input
// val buffer. Returns an offset into buf. User is responsible for remembering
// size of val. We could also store this size inside arena but the encoding and
// decoding will incur some overhead.
func (s *Arena) PutVal(val []byte, meta byte) uint32 {
	l := uint32(len(val)) + 1
	n := atomic.AddUint32(&s.n, l)
	y.AssertTruef(int(n) <= len(s.buf),
		"Arena too small, toWrite:%d newTotal:%d limit:%d",
		l, n, len(s.buf))
	m := n - l
	s.buf[m] = meta
	y.AssertTrue(len(val) == copy(s.buf[m+1:n], val))
	return m
}

func (s *Arena) PutKey(key []byte) uint32 {
	l := uint32(len(key))
	n := atomic.AddUint32(&s.n, l)
	y.AssertTruef(int(n) <= len(s.buf),
		"Arena too small, toWrite:%d newTotal:%d limit:%d",
		l, n, len(s.buf))
	m := n - l
	y.AssertTrue(len(key) == copy(s.buf[m:n], key))
	return m
}

// GetKey returns byte slice at offset.
func (s *Arena) GetKey(offset uint32, size uint16) []byte {
	return s.buf[offset : offset+uint32(size)]
}

// GetVal returns byte slice at offset. The given size should be just the value
// size and should NOT include the meta byte.
func (s *Arena) GetVal(offset uint32, size uint16) ([]byte, byte) {
	return s.buf[offset+1 : offset+1+uint32(size)], s.buf[offset]
}

type ArenaPool struct {
	size int64
	pool chan *Arena // Immutable.
}

func NewArenaPool(size int64, numArenas int) *ArenaPool {
	return &ArenaPool{size, make(chan *Arena, numArenas)}
}

func (s *ArenaPool) Get() *Arena {
	var arena *Arena
	select {
	case arena = <-s.pool:
	default:
		arena = NewArena(s.size)
	}
	return arena
}

func (s *ArenaPool) Put(arena *Arena) {
	arena.Reset()
	select {
	case s.pool <- arena:
	default:
		// Just ignore the returned arena.
	}
}