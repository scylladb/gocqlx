package main

import (
	"fmt"
)

type Stack struct {
	data []interface{}
}

func NewStack() *Stack {
	return &Stack{}
}

func (s *Stack) push(v interface{}) {
	s.data = append(s.data, v)
}

func (s *Stack) pop() (interface{}, error) {
	l := len(s.data)
	if l == 0 {
		return nil, fmt.Errorf("Empty stack")
	}

	v := s.data[l-1]
	s.data = s.data[:l-1]

	return v, nil
}

func (s *Stack) popSlice(count int) ([]interface{}, error) {
	if count == 0 {
		return nil, nil
	}

	l := len(s.data)
	if l < count {
		return nil, fmt.Errorf("Too small stack")
	}

	v := s.data[l-count:]
	s.data = s.data[:l-count]

	return v, nil
}

func (s *Stack) top() (interface{}, error) {
	l := len(s.data)
	if l == 0 {
		return nil, fmt.Errorf("Empty stack")
	}

	return s.data[l-1], nil
}

func (s *Stack) bottom() (interface{}, error) {
	l := len(s.data)
	if l == 0 {
		return nil, fmt.Errorf("Empty stack")
	}

	return s.data[0], nil
}

func (s *Stack) toSlice() []interface{} {
	return s.data
}

func (s *Stack) count() int {
	return len(s.data)
}
