# tuniq

`tuniq` is a Unix command-line utility for streaming frequency analysis.

## Overview

- Replaces `sort | uniq -c | sort -rn | head` with one tool.
- Works on unsorted stdin or multiple files.
- Uses worker sharding for multicore throughput.
- Sorts only unique values for deterministic output.
- Writes results to stdout and diagnostics/stats/progress to stderr.

## Installation

### From source

```bash
go install github.com/flaviomartins/tuniq/cmd/tuniq@latest
```

### From release artifacts

Download binaries from GitHub Releases and place `tuniq` in your `PATH`.

## Building

```bash
make build
```

or:

```bash
go build -o tuniq ./cmd/tuniq
```

## Usage

```bash
tuniq [flags] [file ...]
```

If no files are provided, `tuniq` reads from stdin. Multiple files are processed as one logical stream.

Flags:

- `-n N` top N entries
- `-a` show all entries
- `-r` reverse ordering (ascending count)
- `-c` show counts (default true)
- `-u N`, `--update-every N` live updates every N lines (plain output only)
- `--csv` CSV output
- `--json` JSON output
- `--workers N` worker shards (default: `GOMAXPROCS`)
- `--progress`, `--progress-every`, `--progress-every-seconds` progress cadence
- `--stats`, `--stats-rss` processing stats
- `--memory-limit-bytes` hard stop on estimated counter memory
- `--version`, `--help`

## Examples

### Basic usage

```bash
tuniq -n 10 data.txt
```

### Multiple files as one stream

```bash
tuniq data.0.txt data.1.txt data.2.txt
```

### Live top-N from a stream

```bash
curl -q -sN https://stream.wikimedia.org/v2/stream/recentchange \
  | jq --unbuffered -r '.title // empty' \
  | tuniq -n 20 -u 1
```

### Value-only output

```bash
cat queries.txt | tuniq -c=false
```

### Machine-readable output

```bash
cat queries.txt | tuniq --csv -n 100
cat queries.txt | tuniq --json -n 100
```

## Configuration

`tuniq` loads defaults from these paths in order (later entries override earlier):

1. `~/.tuniqrc` (legacy homedir)
2. `~/.config/tuniq/.tuniq` (or OS equivalent from `os.UserConfigDir`)
3. `./.tuniqrc` (project override)

Config format is `key=value` with `#` comments. CLI flags always override config values.

Supported keys:

```ini
top_n=20
show_all=false
reverse=false
show_count=true
update_every=0
output=plain
workers=8
progress=true
progress_every=500000
progress_every_seconds=0
stats=false
stats_rss=false
memory_limit_bytes=0
```

## XDG directories

Config: ~/.config/tuniq/.tuniq  
State: ~/.local/state/tuniq (or OS equivalent)

## Package layout

- `cmd/tuniq`: CLI entrypoint and signal wiring.
- `pkg/processor`: stream processing orchestration and runtime pipeline.
- `pkg/platform`: OS-specific RSS helpers.
- `pkg/config`, `pkg/output`, `pkg/version`: configuration, output formatting, and build metadata.

## Output

Default plain output:

```text
15234 apple
11201 orange
9321 banana
```

Ordering is deterministic:

1. count descending (or ascending with `-r`)
2. value alphabetical as tiebreak

## Performance and memory

- Counting is shard-local, then merged.
- Sorting happens over unique keys, not total lines.
- Memory growth is proportional to cardinality.
- `--memory-limit-bytes` aborts when estimate exceeds limit (spill-to-disk is not implemented).

## Development

```bash
make fmt
make vet
go test ./...
make build
```

## Contributing

Contributions are welcome. Please run formatting, vet, and tests before opening pull requests.

## License

MIT (see `LICENSE`).
