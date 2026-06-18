# tuniq

Count occurrences of each unique line and display a live top-N leaderboard. Like `sort | uniq -c | sort -rn | head` but faster and streaming — the display updates as data arrives.

## Usage

```
tuniq [-n N] [-f F] [file ...]
```

- `-n N` — show top N lines (default 10)
- `-f F` — flush and redisplay every F lines while streaming (default 10000)
- reads from stdin if no files are given

## Examples

```bash
# top 10 IPs from an access log
tuniq -n 10 access.log

# live top 20 from a stream, leaderboard refreshes every 5000 lines
tail -f access.log | tuniq -n 20 -f 5000

# multiple files, shared counter
tuniq error.log access.log
```

## Install

Download a binary from the [releases page](../../releases), or build from source:

```bash
go build -o tuniq .
```

Requires Go 1.21+.
