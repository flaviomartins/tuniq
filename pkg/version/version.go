package version

// These values are intended to be overridden at build time via -ldflags.
var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)
