package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"

	"github.com/flaviomartins/tuniq/pkg/processor"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), interruptSignals()...)
	defer stop()

	cfg, paths, err := processor.ParseFlags(os.Args[1:], os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "tuniq: %v\n", err)
		os.Exit(2)
	}

	var inputs []io.ReadCloser
	if len(paths) == 0 {
		inputs = append(inputs, io.NopCloser(os.Stdin))
	} else {
		for _, p := range paths {
			f, openErr := os.Open(p)
			if openErr != nil {
				fmt.Fprintf(os.Stderr, "tuniq: %s: %v\n", p, openErr)
				os.Exit(1)
			}
			inputs = append(inputs, f)
		}
	}
	defer func() {
		for _, r := range inputs {
			_ = r.Close()
		}
	}()

	os.Exit(processor.RunWithOptions(ctx, cfg, inputs, os.Stdout, os.Stderr))
}
