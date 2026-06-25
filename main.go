package main

import (
	"bufio"
	"container/heap"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
)

var n = flag.Int("n", 10, "show top N lines")
var f = flag.Int("f", 10000, "flush every F lines")

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: tuniq [-n N] [-f F] [file ...]\n\n")
		fmt.Fprintf(os.Stderr, "Count occurrences of each unique line and print the top N.\n")
		fmt.Fprintf(os.Stderr, "Reads from stdin if no files are given.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}
}

type entry struct {
	key   string
	count uint64
}

func entryBetter(a, b entry) bool {
	if a.count != b.count {
		return a.count > b.count
	}
	return a.key < b.key
}

func entryWorse(a, b entry) bool {
	return entryBetter(b, a)
}

type entryHeap []entry

func (h entryHeap) Len() int { return len(h) }

func (h entryHeap) Less(i, j int) bool {
	return entryWorse(h[i], h[j])
}

func (h entryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *entryHeap) Push(x any) { *h = append(*h, x.(entry)) }

func (h *entryHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func topEntries(counter map[string]*uint64) []entry {
	limit := *n
	if limit <= 0 || len(counter) == 0 {
		return nil
	}

	top := make([]entry, 0, min(limit, len(counter)))
	h := entryHeap{}
	heap.Init(&h)
	for k, v := range counter {
		item := entry{k, *v}
		if len(h) < limit {
			heap.Push(&h, item)
			continue
		}
		if entryBetter(item, h[0]) {
			heap.Pop(&h)
			heap.Push(&h, item)
		}
	}
	for len(h) > 0 {
		top = append(top, heap.Pop(&h).(entry))
	}
	sort.Slice(top, func(i, j int) bool {
		return entryBetter(top[i], top[j])
	})
	return top
}

func printTopEntries(counter map[string]*uint64) {
	top := topEntries(counter)
	w := bufio.NewWriter(os.Stdout)
	w.WriteString("\x1b[2J")
	for _, e := range top {
		fmt.Fprintf(w, "%d %s\n", e.count, e.key)
	}
	w.Flush()
}

func process(r io.Reader, counter map[string]*uint64) {
	scanner := bufio.NewScanner(r)
	lineCount := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		if count, ok := counter[string(line)]; ok {
			*count++
		} else {
			val := uint64(1)
			counter[string(line)] = &val
		}
		lineCount++
		if *f > 0 && lineCount%*f == 0 {
			printTopEntries(counter)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "tuniq: read error: %v\n", err)
	}
}

func main() {
	flag.Parse()
	counter := make(map[string]*uint64, 1<<16)

	if flag.NArg() == 0 {
		process(os.Stdin, counter)
	} else {
		for _, path := range flag.Args() {
			f, err := os.Open(path)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%s: %v\n", path, err)
				continue
			}
			process(f, counter)
			f.Close()
		}
	}

	printTopEntries(counter)
}
