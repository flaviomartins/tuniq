//go:build windows

package platform

func ReadMaxRSSBytes() (uint64, bool) {
	return 0, false
}
