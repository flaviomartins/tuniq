package main

import (
	"context"
	"os"
	"os/signal"

	"github.com/flaviomartins/tuniq/pkg/processor"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), interruptSignals()...)
	defer stop()
	os.Exit(processor.RunContext(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}
