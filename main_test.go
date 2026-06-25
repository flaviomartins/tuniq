package main

import (
	"fmt"
	"strings"
	"testing"
)

func benchmarkCounter(size int) map[string]uint64 {
	counter := make(map[string]uint64, size)
	for i := 0; i < size; i++ {
		counter[fmt.Sprintf("line-%06d", i)] = uint64((i % 1000) + 1)
	}
	return counter
}

func benchmarkInput(size int) string {
	var builder strings.Builder
	builder.Grow(size * len("line-000000\n"))
	for i := 0; i < size; i++ {
		fmt.Fprintf(&builder, "line-%06d\n", i%1000)
	}
	return builder.String()
}

func BenchmarkTopEntries(b *testing.B) {
	counter := benchmarkCounter(100_000)

	oldN := *n
	*n = 20
	defer func() { *n = oldN }()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = topEntries(counter)
	}
}

func BenchmarkProcess(b *testing.B) {
	data := benchmarkInput(100_000)

	oldF := *f
	*f = 0
	defer func() { *f = oldF }()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		counter := make(map[string]uint64, 100_000)
		process(strings.NewReader(data), counter)
	}
}
