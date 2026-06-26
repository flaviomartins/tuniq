package main

import (
	"hash/adler32"
	"hash/crc32"
	"hash/crc64"
	"hash/maphash"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/zeebo/xxh3"
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

// BenchmarkShardIndexXXHash mirrors shardIndex with xxhash (XXH64).
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

// BenchmarkShardIndexXXH3 mirrors shardIndex with XXH3.
func BenchmarkShardIndexXXH3(b *testing.B) {
	line := []byte("line-000042")
	workers := 8
	b.SetBytes(int64(len(line)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = uint64(int(xxh3.Hash(line)) % workers)
	}
}

func BenchmarkHashXXH3(b *testing.B) {
	for _, line := range benchLines {
		line := line
		b.Run(string(line[:min(8, len(line))]), func(b *testing.B) {
			b.SetBytes(int64(len(line)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink = xxh3.Hash(line)
			}
		})
	}
}

var crc32ieeeTable = crc32.MakeTable(crc32.IEEE)
var crc64ecmaTable = crc64.MakeTable(crc64.ECMA)

// BenchmarkShardIndexCRC32C mirrors shardIndex with hardware-accelerated CRC32C (Castagnoli).
func BenchmarkShardIndexCRC32C(b *testing.B) {
	line := []byte("line-000042")
	workers := 8
	b.SetBytes(int64(len(line)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = uint64(int(crc32.Checksum(line, crc32cTable)) % workers)
	}
}

func BenchmarkShardIndexCRC32IEEE(b *testing.B) {
	line := []byte("line-000042")
	workers := 8
	b.SetBytes(int64(len(line)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = uint64(int(crc32.Checksum(line, crc32ieeeTable)) % workers)
	}
}

func BenchmarkShardIndexCRC64(b *testing.B) {
	line := []byte("line-000042")
	workers := 8
	b.SetBytes(int64(len(line)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = uint64(int(crc64.Checksum(line, crc64ecmaTable)) % workers)
	}
}

func BenchmarkShardIndexAdler32(b *testing.B) {
	line := []byte("line-000042")
	workers := 8
	b.SetBytes(int64(len(line)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sink = uint64(int(adler32.Checksum(line)) % workers)
	}
}

func BenchmarkHashCRC32C(b *testing.B) {
	for _, line := range benchLines {
		line := line
		b.Run(string(line[:min(8, len(line))]), func(b *testing.B) {
			b.SetBytes(int64(len(line)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink = uint64(crc32.Checksum(line, crc32cTable))
			}
		})
	}
}

func BenchmarkHashCRC32IEEE(b *testing.B) {
	for _, line := range benchLines {
		line := line
		b.Run(string(line[:min(8, len(line))]), func(b *testing.B) {
			b.SetBytes(int64(len(line)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink = uint64(crc32.Checksum(line, crc32ieeeTable))
			}
		})
	}
}

func BenchmarkHashCRC64(b *testing.B) {
	for _, line := range benchLines {
		line := line
		b.Run(string(line[:min(8, len(line))]), func(b *testing.B) {
			b.SetBytes(int64(len(line)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink = crc64.Checksum(line, crc64ecmaTable)
			}
		})
	}
}

func BenchmarkHashAdler32(b *testing.B) {
	for _, line := range benchLines {
		line := line
		b.Run(string(line[:min(8, len(line))]), func(b *testing.B) {
			b.SetBytes(int64(len(line)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink = uint64(adler32.Checksum(line))
			}
		})
	}
}
