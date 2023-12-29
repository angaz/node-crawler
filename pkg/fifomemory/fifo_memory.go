package fifomemory

import "golang.org/x/exp/slices"

type FIFOMemory[T comparable] struct {
	pos    int
	size   int
	full   bool
	memory []T
}

func New[T comparable](size int) *FIFOMemory[T] {
	return &FIFOMemory[T]{
		pos:    0,
		size:   size,
		full:   false,
		memory: make([]T, 0, size),
	}
}

func (m *FIFOMemory[T]) Push(value T) {
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

func (m *FIFOMemory[T]) Contains(value T) bool {
	return slices.Contains(m.memory, value)
}

func (m *FIFOMemory[T]) ContainsOrPush(value T) bool {
	if slices.Contains(m.memory, value) {
		return true
	}

	m.Push(value)

	return false
}
