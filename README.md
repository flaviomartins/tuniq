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
# top 10 repeated lines from a text file
tuniq -n 10 data.txt

# top 10 repeated lines from split text files
tuniq data.0.txt data.1.txt data.2.txt

# top 10 edited articles from a live Wikimedia change stream
curl -q -sN \
  -H 'Accept: application/json' \
  https://stream.wikimedia.org/v2/stream/recentchange \
  | jq --unbuffered -r 'select(.wiki == "enwiki" and .type == "edit" and .namespace == 0 and .anon != true and .bot != true) | .title' \
  | tuniq -n 10 -f 1

# top 10 editors from the same live Wikimedia change stream
curl -q -sN \
  -H 'Accept: application/json' \
  https://stream.wikimedia.org/v2/stream/recentchange \
  | jq --unbuffered -r 'select(.wiki == "enwiki" and .type == "edit" and .namespace == 0 and .anon != true and .bot != true) | .user' \
  | tuniq -n 10 -f 1

# live top 20 IPs from a stream
tail -F /var/log/nginx/access.log | awk '{print $1}' | tuniq -n 20 -f 5000
```

## Install

Download the latest binary with `curl` or `wget`, or build from source:

### Using curl

```bash
curl -fsSL -o tuniq https://github.com/flaviomartins/tuniq/releases/latest/download/tuniq
chmod +x tuniq
sudo mv tuniq /usr/local/bin/tuniq
sudo ln -sf /usr/local/bin/tuniq /usr/local/bin/tu
```

### Using wget

```bash
wget -qO tuniq https://github.com/flaviomartins/tuniq/releases/latest/download/tuniq
chmod +x tuniq
sudo mv tuniq /usr/local/bin/tuniq
sudo ln -sf /usr/local/bin/tuniq /usr/local/bin/tu
```

```bash
go build -o tuniq .
```

Requires Go 1.21+.
