# tuniq

`tuniq` is a high-performance Unix utility for streaming frequency analysis.

It replaces this common pipeline:

```bash
sort | uniq -c | sort -rn | head
```

with a single command that:

- works on unsorted input
- processes incrementally
- uses multicore workers
- sorts only unique values

## Why tuniq exists

`sort | uniq -c | sort -rn` is robust but expensive for large streams because it must sort the full input first.

`tuniq` counts first and sorts last, so runtime scales with:

- total input lines for counting
- unique keys for sorting

This is especially useful for logs, telemetry streams, and text analytics where input can be arbitrarily long.

## Usage

```bash
tuniq [options] [file ...]
```

If no files are provided, `tuniq` reads from `stdin`.
When multiple files are provided, they are processed as one logical stream.

### CLI options

| Flag | Description |
| --- | --- |
| `-n N` | Show top `N` entries |
| `-u N` | Update every `N` lines in live mode (plain output only) |
| `--update-every N` | Long form of `-u` |
| `-f F`, `--flush-every F` | Backward-compatible aliases for `-u/--update-every` |
| `-a` | Show all entries |
| `-r` | Reverse ordering (ascending count) |
| `-c` | Show counts (default true) |
| `--csv` | Emit CSV output |
| `--json` | Emit JSON output |
| `--stats` | Emit processing stats to `stderr` |
| `--stats-rss` | Include OS peak RSS in stats when available |
| `--progress` | Emit periodic progress to `stderr` |
| `--version` | Print version |
| `--help` | Show help |

### Advanced tuning flags

| Flag | Description |
| --- | --- |
| `--workers N` | Worker shard count (default: `GOMAXPROCS`) |
| `--memory-limit-bytes` | Abort when estimated counter memory exceeds this limit (spill-to-disk not implemented yet) |
| `--progress-every` | Update interval in lines (progress reports and live mode cadence) |
| `-s SECONDS` | Short form of `--progress-every-seconds` |
| `--progress-every-seconds` | Update interval in seconds (works with or without live mode) |

`-u/--update-every` enables live output and sets the line cadence; `--progress-every-seconds` adds a time-based cadence.
In live all-output mode (`-a` + live updates), redraws are throttled and live updates use a bounded preview window to keep terminal/snapshot overhead bounded. Final EOF output remains exact and complete.
Live redraw cadence also adapts automatically when render cost spikes, to avoid runaway terminal overhead.

## Examples

```bash
# top 10 repeated lines from a text file
tuniq -n 10 data.txt

# top 10 repeated lines from split text files
tuniq data.0.txt data.1.txt data.2.txt

# top edited articles from a live Wikimedia change stream
curl -q -sN \
  -H 'Accept: application/json' \
  https://stream.wikimedia.org/v2/stream/recentchange \
  | jq --unbuffered -r 'select(.wiki == "enwiki" and .type == "edit" and .namespace == 0 and .anon != true and .bot != true) | .title' \
  | tuniq -n 20 -u 1

# top editors from the same live Wikimedia stream
curl -q -sN \
  -H 'Accept: application/json' \
  https://stream.wikimedia.org/v2/stream/recentchange \
  | jq --unbuffered -r 'select(.wiki == "enwiki" and .type == "edit" and .namespace == 0 and .anon != true and .bot != true) | .user' \
  | tuniq -n 20 -u 1

# most common IPs in web logs
cat access.log | awk '{print $1}' | tuniq -n 20 -u 5000

# most frequent errors in journald output
journalctl | grep ERROR | tuniq -n 50

# CSV and JSON outputs
cat queries.txt | tuniq --csv -n 100
cat queries.txt | tuniq --json -n 100
```

## Output

Default plain output:

```text
15234 apple
11201 orange
9321 banana
```

The default ordering is:

1. descending count
2. alphabetical tiebreak

With `-r`, count order is reversed while keeping alphabetical tiebreak.

## Streaming and parallel design

Pipeline:

```text
Reader -> Worker shards -> Aggregation -> Sort unique entries -> Output
```

- Reader ingests line-by-line from stdin/files.
- Lines are sharded by hash to avoid lock contention.
- Each worker keeps a local counter.
- Aggregation merges shard results.
- Sorting is done only over unique values.

Deterministic output is guaranteed by explicit final sort ordering.

## Hash table

`tuniq` uses Go's built-in `map[string]*uint64` for counting.
This reduces allocations in duplicate-heavy streams, but high-cardinality inputs (mostly unique lines) remain the worst case for memory and allocation pressure.

Use benchmarks to evaluate performance on your hardware/workload:

```bash
go test -bench . -run ^$
```

## Stats output (`--stats`)

Stats are written to `stderr` and include:

- lines processed
- unique values
- duplicates
- elapsed time
- throughput
- peak counter memory estimate (`peak_counter_estimate_bytes`)
- peak Go heap sys bytes (`peak_heap_sys_bytes`)
- peak RSS (`peak_rss_bytes`, optional via `--stats-rss`)

## Memory behavior

`tuniq` stores only:

- unique line values
- counts

Memory growth is proportional to cardinality (unique keys), not total line count.
`--memory-limit-bytes` is implemented as a hard stop for future spill-to-disk workflows.

## Install

Build from source:

```bash
go build -o tuniq .
```

Requires Go 1.24+.

## Development quality gates

```bash
gofmt -w .
go vet ./...
go test ./...
go test -race ./...
go test -bench . ./...
go build ./...
```

## Benchmarks and datasets

Benchmark with representative datasets:

- all unique
- all duplicates
- Zipf distributions
- Wikipedia titles
- web server logs
- random strings
- very long lines

Compare directly against:

```bash
sort | uniq -c | sort -rn
```

Quick in-repo benchmark targets for workload drift checks:

```bash
go test -run '^$' -bench 'BenchmarkRun(DuplicateHeavy|HighCardinality|ZipfSkewed)$' -benchmem ./...
```

Live-mode overhead and cadence tuning benchmark:

```bash
go test -run '^$' -bench '^BenchmarkLiveModeCadence$' -benchmem -benchtime=2s -count=3 ./...
```

Unix baseline comparison benchmark (`tuniq` vs `sort | uniq -c | sort -rn`):

```bash
go test -run '^$' -bench '^BenchmarkCompareUnixPipeline$' -benchmem ./...
```

This benchmark includes `tuniq-workers1`, `tuniq-workers8`, and `sort-uniq-sort` subcases.
