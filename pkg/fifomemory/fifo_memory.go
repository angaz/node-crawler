package fifomemory

import (
	"sync"

	"golang.org/x/exp/slices"
)

type FIFOMemory[T comparable] struct {
	pos    int
	size   int
	full   bool
	memory []T
	lock   *sync.Mutex
}

func New[T comparable](size int) *FIFOMemory[T] {
	return &FIFOMemory[T]{
		pos:    0,
		size:   size,
		full:   false,
		memory: make([]T, 0, size),
		lock:   &sync.Mutex{},
	}
}

func (m *FIFOMemory[T]) push(value T) {
	if m.full {
		m.memory[m.pos] = value
		m.pos = (m.pos + 1) % m.size
		return
	}

	m.memory = append(m.memory, value)
	m.pos += 1

	if m.pos == m.size {
		m.full = true
		m.pos = 0
	}
}

func (m *FIFOMemory[T]) contains(value T) bool {
	return slices.Contains(m.memory, value)
}

func (m *FIFOMemory[T]) ContainsOrPush(value T) bool {
	m.lock.Lock()
	defer m.lock.Unlock()

	if m.contains(value) {
		return true
	}

	m.push(value)

	return false
}

func (m *FIFOMemory[T]) IsFull() bool {
	return m.full
}
