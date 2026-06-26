//go:build windows

package tuniq

func readMaxRSSBytes() (uint64, bool) {
	return 0, false
}
