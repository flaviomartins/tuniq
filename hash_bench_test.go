package main

import (
	"hash/maphash"
	"testing"

	"github.com/cespare/xxhash/v2"
)

// Representative line lengths for tuniq: short keys dominate real workloads.
var benchLines = [][]byte{
	[]byte("line-000042"),                                   // ~11 bytes – typical duplicate-heavy key
	[]byte("line-012345"),                                   // ~11 bytes
	[]byte("192.168.1.1 - - [26/Jun/2026]"),                // ~30 bytes – log-line prefix
	[]byte("the quick brown fox jumps over the lazy dog"),   // ~43 bytes
}

var sink uint64 // prevent dead-code elimination

func BenchmarkHashMaphash(b *testing.B) {
	seed := maphash.MakeSeed()
	for _, line := range benchLines {
		line := line
		b.Run(string(line[:min(8, len(line))]), func(b *testing.B) {
			b.SetBytes(int64(len(line)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink = maphash.Bytes(seed, line)
			}
		})
	}
}

func BenchmarkHashXXHash(b *testing.B) {
	for _, line := range benchLines {
		line := line
		b.Run(string(line[:min(8, len(line))]), func(b *testing.B) {
			b.SetBytes(int64(len(line)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink = xxhash.Sum64(line)
			}
		})
	}
}

// BenchmarkShardIndexMaphash mirrors the actual shardIndex hot path.
func BenchmarkShardIndexMaphash(b *testing.B) {
	seed := maphash.MakeSeed()
	line := []byte("line-000042")
	workers := 8
	b.SetBytes(int64(len(line)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = uint64(int(maphash.Bytes(seed, line)) % workers)
	}
}

// BenchmarkShardIndexXXHash mirrors shardIndex with xxhash.
func BenchmarkShardIndexXXHash(b *testing.B) {
	line := []byte("line-000042")
	workers := 8
	b.SetBytes(int64(len(line)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = uint64(int(xxhash.Sum64(line)) % workers)
	}
}
