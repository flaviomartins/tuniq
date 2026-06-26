//go:build windows

package main

import "os"

func interruptSignals() []os.Signal {
	return []os.Signal{os.Interrupt}
}
