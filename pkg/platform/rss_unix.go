//go:build !windows

package platform

import (
	"runtime"
	"syscall"
)

func ReadMaxRSSBytes() (uint64, bool) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, false
	}
	rss := uint64(usage.Maxrss)
	// On Linux and Android, Maxrss is reported in KiB.
	if runtime.GOOS == "linux" || runtime.GOOS == "android" {
		rss *= 1024
	}
	return rss, true
}
