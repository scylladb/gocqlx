// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

package gocqlx

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/scylladb/go-reflectx"
)

type tupleBindElement struct {
	base  string
	index int
	count int
}

func parseTupleElementName(name string) (base string, index int, ok bool) {
	if !strings.HasSuffix(name, "]") {
		return "", 0, false
	}

	open := strings.LastIndexByte(name, '[')
	if open <= 0 {
		return "", 0, false
	}

	digits := name[open+1 : len(name)-1]
	if digits == "" || (len(digits) > 1 && digits[0] == '0') {
		return "", 0, false
	}
	for i := range digits {
		if digits[i] < '0' || digits[i] > '9' {
			return "", 0, false
		}
	}

	index, err := strconv.Atoi(digits)
	// The inferred tuple count is index + 1, so the largest int cannot be
	// represented as a valid tuple element index.
	if err != nil || index < 0 || index == math.MaxInt {
		return "", 0, false
	}

	return name[:open], index, true
}

func tupleBindElements(stmt string, names []string) []tupleBindElement {
	if stmt == "" {
		return tupleBindElementsByName(names)
	}

	elements := make([]tupleBindElement, len(names))
	for _, group := range tuplePlaceholderGroups(stmt) {
		if len(group) == 0 || group[len(group)-1] >= len(names) {
			continue
		}

		base, _, ok := parseTupleElementName(names[group[0]])
		if !ok {
			continue
		}
		for index, position := range group {
			nameBase, nameIndex, nameOK := parseTupleElementName(names[position])
			if !nameOK || nameBase != base || nameIndex != index {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}

		for index, position := range group {
			elements[position] = tupleBindElement{base: base, index: index, count: len(group)}
		}
	}
	return elements
}

// tupleBindElementsByName preserves tuple binding when the deprecated Query
// constructor is given a nil gocql.Query. Session-created queries use the CQL
// statement so collection element names such as "items[0]" are not
// misclassified.
func tupleBindElementsByName(names []string) []tupleBindElement {
	type tupleNames struct {
		seen  map[int]int
		count int
	}

	byBase := make(map[string]*tupleNames)
	for _, name := range names {
		base, index, ok := parseTupleElementName(name)
		if !ok {
			continue
		}
		tuple := byBase[base]
		if tuple == nil {
			tuple = &tupleNames{seen: make(map[int]int)}
			byBase[base] = tuple
		}
		tuple.seen[index]++
		if index+1 > tuple.count {
			tuple.count = index + 1
		}
	}

	elements := make([]tupleBindElement, len(names))
	for position, name := range names {
		base, index, ok := parseTupleElementName(name)
		if !ok {
			continue
		}
		tuple := byBase[base]
		if len(tuple.seen) != tuple.count || tuple.seen[index] != 1 {
			continue
		}
		elements[position] = tupleBindElement{base: base, index: index, count: tuple.count}
	}
	return elements
}

type tuplePlaceholderFrame struct {
	positions         []int
	valid             bool
	expectPlaceholder bool
}

func tuplePlaceholderGroups(stmt string) [][]int {
	var (
		groups       [][]int
		stack        []tuplePlaceholderFrame
		placeholder  int
		singleQuote  bool
		doubleQuote  bool
		lineComment  bool
		blockComment bool
	)

	for i := 0; i < len(stmt); i++ {
		ch := stmt[i]
		next := byte(0)
		if i+1 < len(stmt) {
			next = stmt[i+1]
		}

		switch {
		case lineComment:
			if ch == '\n' {
				lineComment = false
			}
			continue
		case blockComment:
			if ch == '*' && next == '/' {
				blockComment = false
				i++
			}
			continue
		case singleQuote:
			if ch == '\'' {
				if next == '\'' {
					i++
				} else {
					singleQuote = false
				}
			}
			continue
		case doubleQuote:
			if ch == '"' {
				if next == '"' {
					i++
				} else {
					doubleQuote = false
				}
			}
			continue
		case ch == '-' && next == '-':
			lineComment = true
			i++
			continue
		case ch == '/' && next == '*':
			blockComment = true
			i++
			continue
		case ch == '\'':
			invalidateTupleFrame(stack)
			singleQuote = true
			continue
		case ch == '"':
			invalidateTupleFrame(stack)
			doubleQuote = true
			continue
		}

		switch ch {
		case '(':
			invalidateTupleFrame(stack)
			stack = append(stack, tuplePlaceholderFrame{
				valid:             isTuplePlaceholderContext(stmt, i),
				expectPlaceholder: true,
			})
		case ')':
			if len(stack) == 0 {
				continue
			}
			frame := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if frame.valid && !frame.expectPlaceholder && len(frame.positions) != 0 {
				groups = append(groups, frame.positions)
			}
		case '?':
			if len(stack) != 0 {
				frame := &stack[len(stack)-1]
				if !frame.expectPlaceholder {
					frame.valid = false
				}
				frame.positions = append(frame.positions, placeholder)
				frame.expectPlaceholder = false
			}
			placeholder++
		case ',':
			if len(stack) != 0 {
				frame := &stack[len(stack)-1]
				if frame.expectPlaceholder {
					frame.valid = false
				}
				frame.expectPlaceholder = true
			}
		case ' ', '\t', '\r', '\n':
		default:
			invalidateTupleFrame(stack)
		}
	}

	return groups
}

func isTuplePlaceholderContext(stmt string, open int) bool {
	previous := open - 1
	for previous >= 0 {
		switch stmt[previous] {
		case ' ', '\t', '\r', '\n':
			previous--
			continue
		}
		break
	}
	if previous < 0 {
		return false
	}

	switch stmt[previous] {
	case '(', ',', '=', '<', '>', '!':
		return true
	}

	wordEnd := previous + 1
	for previous >= 0 {
		ch := stmt[previous]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') {
			previous--
			continue
		}
		break
	}
	switch strings.ToUpper(stmt[previous+1 : wordEnd]) {
	case "IN", "CONTAINS", "KEY", "LIKE":
		return true
	default:
		return false
	}
}

func invalidateTupleFrame(stack []tuplePlaceholderFrame) {
	if len(stack) != 0 {
		stack[len(stack)-1].valid = false
	}
}

func traversalByName(mapper *reflectx.Mapper, t reflect.Type, name string) []int {
	traversals := mapper.TraversalsByName(t, []string{name})
	if len(traversals) == 0 || len(traversals[0]) == 0 {
		return nil
	}
	return traversals[0]
}

func fieldTypeByTraversal(t reflect.Type, traversal []int) reflect.Type {
	t = derefTupleType(t)
	for _, index := range traversal {
		if t.Kind() != reflect.Struct || index >= t.NumField() {
			return nil
		}
		t = t.Field(index).Type
		t = derefTupleType(t)
	}
	return t
}

func isTupleBindContainerType(t reflect.Type) bool {
	return isTupleContainerType(t, marshalerInterface)
}

func isTupleScanContainerType(t reflect.Type) bool {
	return isTupleContainerType(t, unmarshallerInterface)
}

func isTupleContainerType(t, customInterface reflect.Type) bool {
	if t == nil {
		return false
	}
	t = derefTupleType(t)
	if typeImplements(t, customInterface) || (t.Kind() != reflect.Array && t.Kind() != reflect.Slice) {
		return false
	}
	return t.Elem().Kind() != reflect.Uint8
}

func tupleElementByName(mapper *reflectx.Mapper, value reflect.Value, name string, tuple tupleBindElement) (reflect.Value, bool, error) {
	if tuple.count == 0 {
		return reflect.Value{}, false, nil
	}

	traversal := traversalByName(mapper, value.Type(), tuple.base)
	if len(traversal) == 0 {
		return reflect.Value{}, false, nil
	}

	field, err := fieldByTraversalReadOnly(value, traversal)
	if err != nil {
		return reflect.Value{}, false, fmt.Errorf("could not bind tuple element %q: %w", name, err)
	}
	elem, err := tupleBindElementValue(mapper, field, tuple.index, tuple.count)
	if err != nil {
		return reflect.Value{}, false, fmt.Errorf("could not bind tuple element %q: %w", name, err)
	}
	return elem, true, nil
}

func tupleBindElementValue(mapper *reflectx.Mapper, value reflect.Value, index, count int) (reflect.Value, error) {
	t := value.Type()
	switch {
	case isTupleBindContainerType(t):
		return tupleElementValue(value, index, count)
	case isTupleStructType(t, marshalerInterface):
		value, err := derefTupleValue(value, false)
		if err != nil {
			return reflect.Value{}, err
		}
		traversals := tupleStructTraversals(mapper, value.Type())
		if len(traversals) != count {
			return reflect.Value{}, fmt.Errorf("struct %s has %d mapped fields, expected %d tuple elements", value.Type(), len(traversals), count)
		}
		return fieldByTraversalReadOnly(value, traversals[index])
	default:
		return reflect.Value{}, fmt.Errorf("expected a non-byte array, slice, or struct with %d mapped fields but got %s", count, t)
	}
}

func tupleElementValue(value reflect.Value, index, count int) (reflect.Value, error) {
	value, err := derefTupleValue(value, false)
	if err != nil {
		return reflect.Value{}, err
	}

	switch value.Kind() {
	case reflect.Array, reflect.Slice:
		if value.Kind() == reflect.Slice && value.IsNil() {
			return reflect.Value{}, fmt.Errorf("nil slice does not match tuple element count %d", count)
		}
		if value.Len() != count {
			return reflect.Value{}, fmt.Errorf("%s length %d does not match tuple element count %d", value.Kind(), value.Len(), count)
		}
		if index < 0 || index >= value.Len() {
			return reflect.Value{}, fmt.Errorf("tuple element index %d out of range for %s of length %d", index, value.Kind(), value.Len())
		}
		return value.Index(index), nil
	default:
		return reflect.Value{}, fmt.Errorf("expected array or slice but got %s", value.Kind())
	}
}

func tupleElementAddr(value reflect.Value, index, count int) (reflect.Value, error) {
	value, err := derefTupleValue(value, true)
	if err != nil {
		return reflect.Value{}, err
	}

	switch value.Kind() {
	case reflect.Array:
		if value.Len() != count {
			return reflect.Value{}, fmt.Errorf("array length %d does not match tuple element count %d", value.Len(), count)
		}
	case reflect.Slice:
		if !value.CanSet() {
			return reflect.Value{}, fmt.Errorf("cannot allocate unsettable slice %s", value.Type())
		}
		// Allocate for every row so retained StructScan results do not share a
		// backing array that is overwritten by the next scan. Tuple elements
		// are planned in index order, so only the first element starts a row.
		if index == 0 || value.Len() != count {
			value.Set(reflect.MakeSlice(value.Type(), count, count))
		}
	default:
		return reflect.Value{}, fmt.Errorf("expected array or slice but got %s", value.Kind())
	}

	if index < 0 || index >= value.Len() {
		return reflect.Value{}, fmt.Errorf("tuple element index %d out of range for %s of length %d", index, value.Kind(), value.Len())
	}
	elem := value.Index(index)
	if !elem.CanAddr() {
		return reflect.Value{}, fmt.Errorf("cannot address tuple element %d in %s", index, value.Type())
	}
	return elem.Addr(), nil
}

func tupleStructTraversals(mapper *reflectx.Mapper, t reflect.Type) [][]int {
	t = derefTupleType(t)
	if t.Kind() != reflect.Struct {
		return nil
	}

	mapping := mapper.TypeMap(t)
	var traversals [][]int
	var appendFields func([]*reflectx.FieldInfo)
	appendFields = func(fields []*reflectx.FieldInfo) {
		for _, field := range fields {
			if field == nil {
				continue
			}
			if field.Embedded {
				appendFields(field.Children)
				continue
			}
			if visible, ok := mapping.Names[field.Path]; !ok || visible != field {
				continue
			}
			traversals = append(traversals, append([]int(nil), field.Index...))
		}
	}
	appendFields(mapping.Tree.Children)
	return traversals
}

func isTupleStructType(t, customInterface reflect.Type) bool {
	if t == nil {
		return false
	}
	t = derefTupleType(t)
	return t.Kind() == reflect.Struct && !typeImplements(t, customInterface)
}

func typeImplements(t, iface reflect.Type) bool {
	return t.Implements(iface) || reflect.PointerTo(t).Implements(iface)
}

func derefTupleType(t reflect.Type) reflect.Type {
	for t != nil && t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

func fieldByTraversalReadOnly(value reflect.Value, traversal []int) (reflect.Value, error) {
	for _, index := range traversal {
		value = reflect.Indirect(value)
		if !value.IsValid() {
			return reflect.Value{}, fmt.Errorf("nil pointer in field traversal")
		}
		if value.Kind() != reflect.Struct || index < 0 || index >= value.NumField() {
			return reflect.Value{}, fmt.Errorf("invalid field traversal")
		}
		value = value.Field(index)
	}
	return value, nil
}

func derefTupleValue(value reflect.Value, allocate bool) (reflect.Value, error) {
	if !value.IsValid() {
		return reflect.Value{}, fmt.Errorf("expected array or slice but got nil")
	}

	for value.Kind() == reflect.Ptr {
		if value.IsNil() {
			if !allocate {
				return reflect.Value{}, fmt.Errorf("nil %s", value.Type())
			}
			if !value.CanSet() {
				return reflect.Value{}, fmt.Errorf("cannot allocate unsettable %s", value.Type())
			}
			value.Set(reflect.New(value.Type().Elem()))
		}
		value = value.Elem()
	}

	return value, nil
}

func tupleElementFromMap(mapper *reflectx.Mapper, m map[string]interface{}, name string, tuple tupleBindElement) (elem interface{}, ok bool, err error) {
	if tuple.count == 0 {
		return nil, false, nil
	}

	value, ok := m[tuple.base]
	if !ok {
		return nil, false, nil
	}

	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return nil, false, fmt.Errorf("could not bind tuple element %q: expected a tuple value in %q but got nil", name, tuple.base)
	}

	element, err := tupleBindElementValue(mapper, rv, tuple.index, tuple.count)
	if err != nil {
		return nil, false, fmt.Errorf("could not bind tuple element %q: %w", name, err)
	}
	return element.Interface(), true, nil
}
