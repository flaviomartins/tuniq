package main

import (
	"bufio"
	"cmp"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
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

func printTop(counter map[string]uint64) {
	top := make([]entry, 0, len(counter))
	for k, v := range counter {
		top = append(top, entry{k, v})
	}
	slices.SortFunc(top, func(a, b entry) int {
		return cmp.Compare(b.count, a.count)
	})
	if len(top) > *n {
		top = top[:*n]
	}
	w := bufio.NewWriter(os.Stdout)
	w.WriteString("\x1b[2J")
	for _, e := range top {
		fmt.Fprintf(w, "%d %s\n", e.count, e.key)
	}
	w.Flush()
}

func process(r io.Reader, counter map[string]uint64) {
	scanner := bufio.NewReaderSize(r, 1<<20)
	cnt := 0
	for {
		line, err := scanner.ReadString('\n')
		if len(line) > 0 {
			if line[len(line)-1] == '\n' {
				line = line[:len(line)-1]
			}
			counter[line]++
			cnt++
			if cnt%*f == 0 {
				printTop(counter)
			}
		}
		if err != nil {
			break
		}
	}
}

func main() {
	flag.Parse()
	counter := make(map[string]uint64, 1<<16)

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

	printTop(counter)
}
