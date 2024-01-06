package fifomemory

import (
	"testing"

	"golang.org/x/exp/slices"
)

func TestFIFOMemory(t *testing.T) {
	mem := New[int](3)

	if !slices.Equal(mem.memory, []int{}) {
		t.Errorf("slices not equal: expected: %v, got: %v", []int{}, mem.memory)
	}

	if mem.Contains(0) {
		t.Errorf("mem should not contain 0. mem: %v", mem.memory)
	}

	mem.ContainsOrPush(1)
	if !slices.Equal(mem.memory, []int{1}) {
		t.Errorf("slices not equal: expected: %v, got: %v", []int{1}, mem.memory)
	}
	if mem.Contains(0) {
		t.Errorf("mem should not contain 0. mem: %v", mem.memory)
	}
	if !mem.Contains(1) {
		t.Errorf("mem should contain 1. mem: %v", mem.memory)
	}

	mem.ContainsOrPush(2)
	if !slices.Equal(mem.memory, []int{1, 2}) {
		t.Errorf("slices not equal: expected: %v, got: %v", []int{1, 2}, mem.memory)
	}

	mem.ContainsOrPush(3)
	if !slices.Equal(mem.memory, []int{1, 2, 3}) {
		t.Errorf("slices not equal: expected: %v, got: %v", []int{1, 2, 3}, mem.memory)
	}

	mem.ContainsOrPush(4)
	if !slices.Equal(mem.memory, []int{4, 2, 3}) {
		t.Errorf("slices not equal: expected: %v, got: %v", []int{4, 2, 3}, mem.memory)
	}

	mem.ContainsOrPush(5)
	if !slices.Equal(mem.memory, []int{4, 5, 3}) {
		t.Errorf("slices not equal: expected: %v, got: %v", []int{4, 5, 3}, mem.memory)
	}

	if mem.Contains(0) {
		t.Errorf("mem should not contain 0. mem: %v", mem.memory)
	}
	if !mem.Contains(4) {
		t.Errorf("mem should contain 4. mem: %v", mem.memory)
	}
	if !mem.Contains(5) {
		t.Errorf("mem should contain 4. mem: %v", mem.memory)
	}
	if !mem.Contains(3) {
		t.Errorf("mem should contain 4. mem: %v", mem.memory)
	}
}
