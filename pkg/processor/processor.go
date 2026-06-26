package processor

import (
	"bufio"
	"container/heap"
	"context"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flaviomartins/tuniq/pkg/config"
	"github.com/flaviomartins/tuniq/pkg/output"
	"github.com/flaviomartins/tuniq/pkg/platform"
	"github.com/flaviomartins/tuniq/pkg/version"
)

const liveAllMinRenderInterval = 250 * time.Millisecond
const liveAllPreviewTopN = 1000
const maxBatchBytes = 64 * 1024
const maxAdaptiveBatchFactor = 4
const liveRenderMaxInterval = 2 * time.Second
const memCheckEveryBatches = 16
const liveChannelPollEveryLines = 64
const counterGrowthProbeLines = 32_768
const counterGrowthMinUnique = 8_192
const counterGrowthMinUniqueRatio = 0.40
const counterGrowthTargetFactor = 2

type options struct {
	topN             int
	showAll          bool
	reverse          bool
	showCount        bool
	flushEvery       int
	stats            bool
	statsRSS         bool
	progress         bool
	workers          int
	memoryLimitBytes uint64
	progressEvery    uint64
	progressSeconds  float64
	output           output.Mode
}

type entry struct {
	value string
	count uint64
}

type runStats struct {
	lines            uint64
	unique           uint64
	elapsed          time.Duration
	throughput       float64
	peakCounterBytes uint64
	peakHeapSysBytes uint64
	peakRSSBytes     uint64
	rssAvailable     bool
	rssRequested     bool
	duplicates       uint64
}

type counter interface {
	Inc([]byte)
	Entries() []entry
	Len() int
	EstimatedBytes() uint64
}

type mapCounter struct {
	values map[string]*uint64
}

func newMapCounter(capacity int) *mapCounter {
	if capacity < 16 {
		capacity = 16
	}
	return &mapCounter{values: make(map[string]*uint64, capacity)}
}

func (m *mapCounter) Inc(line []byte) {
	if count, ok := m.values[string(line)]; ok {
		*count++
		return
	}
	key := string(line)
	count := uint64(1)
	m.values[key] = &count
}

func (m *mapCounter) IncTracked(line []byte) (count uint64, insertedKey string, inserted bool) {
	if ptr, ok := m.values[string(line)]; ok {
		*ptr++
		return *ptr, "", false
	}
	key := string(line)
	v := uint64(1)
	m.values[key] = &v
	return v, key, true
}

func (m *mapCounter) Entries() []entry {
	out := make([]entry, 0, len(m.values))
	return m.EntriesInto(out)
}

func (m *mapCounter) EntriesInto(dst []entry) []entry {
	out := dst[:0]
	for k, v := range m.values {
		out = append(out, entry{value: k, count: *v})
	}
	return out
}

func (m *mapCounter) TopNSortedInto(n int, reverse bool, dst []entry) []entry {
	if n <= 0 || len(m.values) == 0 {
		return dst[:0]
	}
	if n >= len(m.values) {
		out := m.EntriesInto(dst)
		sortEntries(out, reverse)
		return out
	}

	items := dst[:0]
	if cap(items) < n {
		items = make([]entry, 0, n)
	}
	h := &topNHeap{
		reverse: reverse,
		items:   items,
	}
	heap.Init(h)
	for k, v := range m.values {
		item := entry{value: k, count: *v}
		if h.Len() < n {
			heap.Push(h, item)
			continue
		}
		if entryBetter(item, h.items[0], reverse) {
			heap.Pop(h)
			heap.Push(h, item)
		}
	}
	sortEntries(h.items, reverse)
	return h.items
}

func (m *mapCounter) Len() int {
	return len(m.values)
}

func (m *mapCounter) EstimatedBytes() uint64 {
	var total uint64
	for k := range m.values {
		total += uint64(len(k)) + 16
	}
	return total
}

func (m *mapCounter) Grow(capacity int) {
	if capacity <= len(m.values) {
		return
	}
	grown := make(map[string]*uint64, capacity)
	for k, v := range m.values {
		grown[k] = v
	}
	m.values = grown
}

type workerResult struct {
	entries []entry
	unique  int
	bytes   uint64
}

type liveTopNTracker struct {
	reverse bool
	limit   int
	items   []entry
	pos     map[string]int
}

func newLiveTopNTracker(limit int, reverse bool) *liveTopNTracker {
	return &liveTopNTracker{
		reverse: reverse,
		limit:   limit,
		items:   make([]entry, 0, limit),
		pos:     make(map[string]int, limit),
	}
}

func (t liveTopNTracker) Len() int { return len(t.items) }

func (t liveTopNTracker) Less(i, j int) bool {
	return entryWorse(t.items[i], t.items[j], t.reverse)
}

func (t liveTopNTracker) Swap(i, j int) {
	t.items[i], t.items[j] = t.items[j], t.items[i]
	t.pos[t.items[i].value] = i
	t.pos[t.items[j].value] = j
}

func (t *liveTopNTracker) Push(x any) {
	e := x.(entry)
	t.pos[e.value] = len(t.items)
	t.items = append(t.items, e)
}

func (t *liveTopNTracker) Pop() any {
	n := len(t.items)
	e := t.items[n-1]
	t.items = t.items[:n-1]
	delete(t.pos, e.value)
	return e
}

func (t *liveTopNTracker) SnapshotSortedInto(dst []entry) []entry {
	out := dst[:0]
	if cap(out) < len(t.items) {
		out = make([]entry, 0, len(t.items))
	}
	out = append(out, t.items...)
	sortEntries(out, t.reverse)
	return out
}

func (t *liveTopNTracker) UpdateLine(line []byte, count uint64, insertedKey string, inserted bool) {
	if t.limit <= 0 {
		return
	}
	if idx, ok := t.pos[string(line)]; ok {
		t.items[idx].count = count
		heap.Fix(t, idx)
		return
	}
	candidateKey := ""
	if len(t.items) >= t.limit {
		worst := t.items[0]
		if t.reverse {
			if count > worst.count {
				return
			}
		} else {
			if count < worst.count {
				return
			}
		}
		if count == worst.count {
			if inserted {
				candidateKey = insertedKey
			} else {
				candidateKey = string(line)
			}
			if candidateKey >= worst.value {
				return
			}
		}
	}
	if candidateKey == "" {
		if inserted {
			candidateKey = insertedKey
		} else {
			candidateKey = string(line)
		}
	}
	candidate := entry{value: candidateKey, count: count}
	if len(t.items) < t.limit {
		heap.Push(t, candidate)
		return
	}
	heap.Pop(t)
	heap.Push(t, candidate)
}

type lineBatch struct {
	slab []byte
	refs []lineRef
}

type lineRef struct {
	start int
	end   int
}

type snapshotRequest struct {
	topN    int
	showAll bool
	reverse bool
}

type liveRenderer struct {
	w           io.Writer
	bw          *bufio.Writer
	showCount   bool
	initialized bool
	prevEntries []entry
	ansiScratch []byte
	dirtyBitmap []uint64
	frameBuf    []byte
}

type liveRenderControl struct {
	baseInterval     time.Duration
	adaptiveInterval time.Duration
	lastRender       time.Time
}

func newLiveRenderControl(base time.Duration) *liveRenderControl {
	return &liveRenderControl{
		baseInterval:     base,
		adaptiveInterval: base,
	}
}

func (c *liveRenderControl) currentInterval() time.Duration {
	return maxDuration(c.baseInterval, c.adaptiveInterval)
}

func (c *liveRenderControl) shouldRender(now time.Time) bool {
	interval := c.currentInterval()
	if interval == 0 {
		return true
	}
	if c.lastRender.IsZero() {
		return true
	}
	return now.Sub(c.lastRender) >= interval
}

func (c *liveRenderControl) recordRender(now time.Time, renderDuration time.Duration) {
	c.lastRender = now
	if renderDuration <= 0 {
		return
	}
	target := maxDuration(c.baseInterval, 4*renderDuration)
	if target > liveRenderMaxInterval {
		target = liveRenderMaxInterval
	}
	if c.adaptiveInterval == 0 {
		c.adaptiveInterval = target
		return
	}
	// Smooth interval updates so redraw cadence doesn't oscillate.
	c.adaptiveInterval = (c.adaptiveInterval*3 + target) / 4
}

func newLiveRenderer(w io.Writer, opts options) *liveRenderer {
	return &liveRenderer{
		w:           w,
		bw:          bufio.NewWriterSize(w, 256*1024),
		showCount:   opts.showCount,
		ansiScratch: make([]byte, 0, 32),
		frameBuf:    make([]byte, 0, 4096),
	}
}

func Run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	return RunContext(context.Background(), args, stdin, stdout, stderr)
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	return Run(args, stdin, stdout, stderr)
}

func RunContext(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	opts, paths, err := parseFlags(args, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintf(stderr, "tuniq: %v\n", err)
		return 2
	}
	if opts.workers < 1 {
		fmt.Fprintln(stderr, "tuniq: workers must be greater than zero")
		return 2
	}
	if opts.flushEvery < 0 {
		fmt.Fprintln(stderr, "tuniq: -u/--update-every must be zero or greater")
		return 2
	}
	if opts.flushEvery > 0 && opts.output != output.ModePlain {
		fmt.Fprintln(stderr, "tuniq: -u/--update-every is only supported with plain output")
		return 2
	}
	if opts.progressSeconds < 0 {
		fmt.Fprintln(stderr, "tuniq: --progress-every-seconds must be zero or greater")
		return 2
	}
	if opts.flushEvery > 0 {
		opts.progressEvery = uint64(opts.flushEvery)
	}
	if opts.progressEvery == 0 && opts.progressSeconds == 0 {
		fmt.Fprintln(stderr, "tuniq: at least one update cadence must be configured")
		return 2
	}

	var inputReaders []io.ReadCloser
	if len(paths) == 0 {
		inputReaders = append(inputReaders, io.NopCloser(stdin))
	} else {
		for _, p := range paths {
			f, openErr := os.Open(p)
			if openErr != nil {
				fmt.Fprintf(stderr, "tuniq: %s: %v\n", p, openErr)
				return 1
			}
			inputReaders = append(inputReaders, f)
		}
	}
	defer func() {
		for _, r := range inputReaders {
			_ = r.Close()
		}
	}()

	start := time.Now()
	var renderer *liveRenderer
	if opts.flushEvery > 0 {
		renderer = newLiveRenderer(stdout, opts)
	}
	shards, stats, processErr := processStream(ctx, inputReaders, opts, renderer, stderr)
	if processErr != nil {
		fmt.Fprintf(stderr, "tuniq: %v\n", processErr)
		return 1
	}
	results := finalizeFromShards(shards, opts)
	if opts.flushEvery > 0 {
		if len(results) > 0 && !renderer.matches(results) {
			if renderErr := renderer.render(results); renderErr != nil {
				fmt.Fprintf(stderr, "tuniq: write error: %v\n", renderErr)
				return 1
			}
		}
	} else {
		if writeErr := writeOutput(stdout, results, opts); writeErr != nil {
			fmt.Fprintf(stderr, "tuniq: write error: %v\n", writeErr)
			return 1
		}
	}

	stats.elapsed = time.Since(start)
	if stats.elapsed > 0 {
		stats.throughput = float64(stats.lines) / stats.elapsed.Seconds()
	}
	if stats.lines >= stats.unique {
		stats.duplicates = stats.lines - stats.unique
	}
	if opts.stats {
		writeStats(stderr, stats)
	}
	return 0
}

func parseFlags(args []string, stderr io.Writer) (options, []string, error) {
	defaults, err := config.LoadDefault()
	if err != nil {
		return options{}, nil, err
	}

	fs := flag.NewFlagSet("tuniq", flag.ContinueOnError)
	fs.SetOutput(stderr)

	opts := options{
		topN:             defaults.TopN,
		showAll:          defaults.ShowAll,
		reverse:          defaults.Reverse,
		showCount:        defaults.ShowCount,
		flushEvery:       defaults.UpdateEvery,
		stats:            defaults.Stats,
		statsRSS:         defaults.StatsRSS,
		progress:         defaults.Progress,
		workers:          defaults.Workers,
		memoryLimitBytes: defaults.MemoryLimitBytes,
		progressEvery:    defaults.ProgressEvery,
		progressSeconds:  defaults.ProgressSeconds,
		output:           defaults.OutputMode,
	}

	var showVersion bool
	var csvOut bool
	var jsonOut bool

	fs.IntVar(&opts.topN, "n", opts.topN, "show top N entries")
	fs.IntVar(&opts.flushEvery, "u", opts.flushEvery, "update every N lines in live mode (plain output only)")
	fs.IntVar(&opts.flushEvery, "update-every", opts.flushEvery, "long form for -u")
	fs.IntVar(&opts.flushEvery, "f", opts.flushEvery, "deprecated alias for -u")
	fs.IntVar(&opts.flushEvery, "flush-every", opts.flushEvery, "deprecated alias for --update-every")
	fs.BoolVar(&opts.showAll, "a", opts.showAll, "show all entries (live mode shows throttled preview; EOF output is complete)")
	fs.BoolVar(&opts.reverse, "r", opts.reverse, "reverse ordering (ascending count)")
	fs.BoolVar(&opts.showCount, "c", opts.showCount, "show counts")
	fs.BoolVar(&csvOut, "csv", false, "output CSV")
	fs.BoolVar(&jsonOut, "json", false, "output JSON")
	fs.BoolVar(&opts.stats, "stats", opts.stats, "write processing statistics to stderr")
	fs.BoolVar(&opts.statsRSS, "stats-rss", opts.statsRSS, "include OS-reported peak RSS in stats when available")
	fs.BoolVar(&opts.progress, "progress", opts.progress, "write periodic progress to stderr")
	fs.BoolVar(&showVersion, "version", false, "print version and exit")
	fs.IntVar(&opts.workers, "workers", opts.workers, "number of worker shards")
	fs.Uint64Var(&opts.memoryLimitBytes, "memory-limit-bytes", opts.memoryLimitBytes, "memory limit before aborting (spill-to-disk not implemented)")
	fs.Uint64Var(&opts.progressEvery, "progress-every", opts.progressEvery, "progress interval in lines")
	fs.Float64Var(&opts.progressSeconds, "s", opts.progressSeconds, "short form for --progress-every-seconds")
	fs.Float64Var(&opts.progressSeconds, "progress-every-seconds", opts.progressSeconds, "update interval in seconds")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "Usage: tuniq [options] [file ...]")
		fmt.Fprintln(stderr, "")
		fmt.Fprintln(stderr, "Streaming frequency analysis for unsorted input.")
		fmt.Fprintln(stderr, "")
		fs.PrintDefaults()
		fmt.Fprintln(stderr, "")
		fmt.Fprintf(
			stderr,
			"Live mode note: when combining -a with live updates (-u/--update-every),\n"+
				"streaming display is a throttled top-%d preview to keep overhead bounded.\n"+
				"Final EOF output remains exact and complete.\n",
			liveAllPreviewTopN,
		)
	}

	if err := fs.Parse(args); err != nil {
		return options{}, nil, err
	}
	if showVersion {
		fmt.Fprintf(stderr, "tuniq %s\n", version.Version)
		return options{}, nil, flag.ErrHelp
	}
	modeFlagCount := 0
	csvSet, jsonSet := false, false
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "csv":
			csvSet = true
		case "json":
			jsonSet = true
		}
	})
	if csvSet {
		modeFlagCount++
	}
	if jsonSet {
		modeFlagCount++
	}
	if modeFlagCount > 1 {
		return options{}, nil, errors.New("--csv and --json are mutually exclusive")
	}
	if csvSet && csvOut {
		opts.output = output.ModeCSV
	} else if jsonSet && jsonOut {
		opts.output = output.ModeJSON
	}
	if opts.progressEvery == 0 && opts.progressSeconds == 0 {
		return options{}, nil, errors.New("progress-every or progress-every-seconds must be greater than zero")
	}
	return opts, fs.Args(), nil
}

func processStream(ctx context.Context, inputs []io.ReadCloser, opts options, renderer *liveRenderer, stderr io.Writer) ([][]entry, runStats, error) {
	stats := runStats{}
	workerCount := opts.workers
	liveMode := opts.flushEvery > 0
	useIncrementalShardTopN := opts.topN > 0 && !opts.showAll
	liveTopN, liveShowAll := liveSnapshotSelection(opts)
	liveBoundedSnapshot := liveMode && !liveShowAll && liveTopN >= 0
	liveUseIncrementalTopN := liveBoundedSnapshot && liveTopN > 0
	batchSize := computeBatchLineTarget(workerCount, liveMode)
	counterInitCap := initialCounterCapacity(opts, workerCount, liveMode)
	updateEnabled := liveMode || opts.progress
	var secondsInterval time.Duration
	var liveRenderCtrl *liveRenderControl
	var lastUpdate time.Time
	nextLineUpdate := uint64(0)
	if updateEnabled {
		secondsInterval = durationFromSeconds(opts.progressSeconds)
		if liveMode {
			liveRenderInterval := effectiveLiveRenderInterval(opts, secondsInterval)
			liveRenderCtrl = newLiveRenderControl(liveRenderInterval)
		}
		lastUpdate = time.Now()
		nextLineUpdate = opts.progressEvery
		if nextLineUpdate == 0 {
			nextLineUpdate = ^uint64(0)
		}
	}

	lineCounts := atomic.Uint64{}
	peakHeapSys := atomic.Uint64{}
	peakRSS := atomic.Uint64{}
	rssAvailable := atomic.Bool{}
	jobChans := make([]chan *lineBatch, workerCount)
	batchPool := sync.Pool{
		New: func() any {
			return &lineBatch{
				slab: make([]byte, 0, maxBatchBytes),
				refs: make([]lineRef, 0, batchSize),
			}
		},
	}
	pendingBatches := make([]*lineBatch, workerCount)
	snapshotReqChans := make([]chan snapshotRequest, workerCount)
	snapshotResponses := make([]chan []entry, workerCount)
	snapshotShards := make([][]entry, workerCount)
	snapshotDirty := make([]atomic.Bool, workerCount)
	renderReqCh := make(chan struct{}, 1)
	renderErrCh := make(chan error, 1)
	renderDurCh := make(chan time.Duration, 1)
	resultCh := make(chan workerResult, workerCount)
	errCh := make(chan error, workerCount+1)
	stopCh := make(chan struct{})
	workerFail := newWorkerFailure()

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		jobChans[i] = make(chan *lineBatch, 256)
		if liveMode {
			snapshotReqChans[i] = make(chan snapshotRequest, 1)
			snapshotResponses[i] = make(chan []entry, 1)
		}
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			memLimit := uint64(0)
			if opts.memoryLimitBytes > 0 {
				memLimit = opts.memoryLimitBytes / uint64(workerCount)
				if memLimit == 0 {
					memLimit = 1
				}
			}
			if liveMode {
				c := newMapCounter(counterInitCap)
				snapshotReq := snapshotReqChans[workerID]
				snapshotResp := snapshotResponses[workerID]
				var snapshotFullBuf []entry
				var snapshotTopBuf []entry
				memCheckCountdown := memCheckEveryBatches
				var growthState counterGrowthState
				var tracker *liveTopNTracker
				if liveUseIncrementalTopN || useIncrementalShardTopN {
					trackerLimit := liveTopN
					if useIncrementalShardTopN {
						trackerLimit = opts.topN
					}
					tracker = newLiveTopNTracker(trackerLimit, opts.reverse)
					heap.Init(tracker)
				}
				for {
					select {
					case batch, ok := <-jobChans[workerID]:
						if !ok {
							if err := finalMemoryLimitCheck(c, memLimit); err != nil {
								workerFail.Fail(err)
								errCh <- err
								return
							}
							finalEntries := c.Entries()
							if useIncrementalShardTopN && tracker != nil {
								finalEntries = tracker.SnapshotSortedInto(nil)
							}
							resultCh <- workerResult{
								entries: finalEntries,
								unique:  c.Len(),
								bytes:   c.EstimatedBytes(),
							}
							return
						}
						processBatchIntoCounter(c, tracker, batch, &growthState, memLimit)
						snapshotDirty[workerID].Store(true)
						recycleLineBatch(batch)
						batchPool.Put(batch)
						if err := checkMemoryLimit(c, memLimit, &memCheckCountdown); err != nil {
							workerFail.Fail(err)
							errCh <- err
							return
						}
					case req := <-snapshotReq:
						for len(jobChans[workerID]) > 0 {
							batch := <-jobChans[workerID]
							processBatchIntoCounter(c, tracker, batch, &growthState, memLimit)
							snapshotDirty[workerID].Store(true)
							recycleLineBatch(batch)
							batchPool.Put(batch)
							if err := checkMemoryLimit(c, memLimit, &memCheckCountdown); err != nil {
								workerFail.Fail(err)
								errCh <- err
								return
							}
						}
						if !req.showAll && req.topN >= 0 {
							if req.topN == 0 {
								snapshotTopBuf = snapshotTopBuf[:0]
								snapshotResp <- snapshotTopBuf
								break
							}
						}
						if tracker != nil && !req.showAll && req.topN >= 0 {
							snapshotTopBuf = tracker.SnapshotSortedInto(snapshotTopBuf)
							snapshotResp <- snapshotTopBuf
							break
						}
						if req.showAll || req.topN < 0 {
							snapshotFullBuf = c.EntriesInto(snapshotFullBuf)
							sortEntries(snapshotFullBuf, req.reverse)
							snapshotResp <- snapshotFullBuf
							break
						}
						// Defensive fallback when bounded snapshots are requested before
						// tracker initialization; hot path should use tracker snapshots.
						snapshotTopBuf = c.TopNSortedInto(req.topN, req.reverse, snapshotTopBuf)
						snapshotResp <- snapshotTopBuf
					}
				}
			}
			c := newMapCounter(counterInitCap)
			memCheckCountdown := memCheckEveryBatches
			var growthState counterGrowthState
			var tracker *liveTopNTracker
			if useIncrementalShardTopN {
				tracker = newLiveTopNTracker(opts.topN, opts.reverse)
				heap.Init(tracker)
			}
			for batch := range jobChans[workerID] {
				processBatchIntoCounter(c, tracker, batch, &growthState, memLimit)
				recycleLineBatch(batch)
				batchPool.Put(batch)
				if err := checkMemoryLimit(c, memLimit, &memCheckCountdown); err != nil {
					workerFail.Fail(err)
					errCh <- err
					return
				}
			}
			if err := finalMemoryLimitCheck(c, memLimit); err != nil {
				workerFail.Fail(err)
				errCh <- err
				return
			}
			finalEntries := c.Entries()
			if tracker != nil {
				finalEntries = tracker.SnapshotSortedInto(nil)
			}
			resultCh <- workerResult{
				entries: finalEntries,
				unique:  c.Len(),
				bytes:   c.EstimatedBytes(),
			}
		}(i)
	}
	var renderWG sync.WaitGroup
	if liveMode {
		renderWG.Add(1)
		go func() {
			defer renderWG.Done()
			var snapshotMergeBuf []entry
			var snapshotNodeBuf []mergeNode
			var snapshotRequested []bool
			for range renderReqCh {
				renderStart := time.Now()
				snapshotMergeBuf, snapshotNodeBuf, snapshotRequested = snapshotEntriesFromWorkers(
					snapshotReqChans,
					snapshotResponses,
					snapshotShards,
					snapshotDirty,
					opts,
					snapshotMergeBuf,
					snapshotNodeBuf,
					snapshotRequested,
				)
				if err := renderer.render(snapshotMergeBuf); err != nil {
					select {
					case renderErrCh <- err:
					default:
					}
					return
				}
				select {
				case renderDurCh <- time.Since(renderStart):
				default:
				}
			}
		}()
	}

	sampleMemory := opts.stats || opts.statsRSS || opts.memoryLimitBytes > 0
	var done chan struct{}
	if sampleMemory {
		done = make(chan struct{})
		go func() {
			defer close(done)
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
					var m runtime.MemStats
					runtime.ReadMemStats(&m)
					for {
						current := peakHeapSys.Load()
						if m.Sys <= current || peakHeapSys.CompareAndSwap(current, m.Sys) {
							break
						}
					}
					if opts.statsRSS {
						if rss, ok := platform.ReadMaxRSSBytes(); ok {
							rssAvailable.Store(true)
							setAtomicMax(&peakRSS, rss)
						}
					}
				}
			}
		}()
	}

	var sendErr error
	if !updateEnabled {
		sendErr = streamLinesDispatch(
			ctx,
			inputs,
			workerCount,
			batchSize,
			&lineCounts,
			pendingBatches,
			&batchPool,
			jobChans,
			workerFail,
		)
	} else {
		sendErr = streamLines(ctx, inputs, func(line []byte) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			if err := workerFail.Err(); err != nil {
				return err
			}
			lines := lineCounts.Add(1)
			if liveMode && lines%liveChannelPollEveryLines == 0 {
				if err := pollLiveChannels(ctx, stopCh, liveRenderCtrl, renderDurCh, renderErrCh); err != nil {
					return err
				}
			}
			var emitUpdate bool
			var now time.Time
			if secondsInterval == 0 {
				emitUpdate = shouldEmitLineUpdate(lines, &nextLineUpdate, opts.progressEvery)
				if emitUpdate && liveMode {
					now = time.Now()
				}
			} else {
				now = time.Now()
				emitUpdate = shouldEmitUpdate(lines, now, &nextLineUpdate, &lastUpdate, opts.progressEvery, secondsInterval)
			}
			idx := shardIndex(line, workerCount)
			if err := appendLineToPendingBatch(line, idx, pendingBatches, &batchPool, jobChans, batchSize, liveMode, workerFail); err != nil {
				return err
			}
			if emitUpdate {
				if liveMode {
					if err := flushPendingBatches(pendingBatches, &batchPool, jobChans, workerFail); err != nil {
						return err
					}
					drainRenderDurations(liveRenderCtrl, renderDurCh)
					if err := pollRenderError(renderErrCh); err != nil {
						return err
					}
					if now.IsZero() {
						now = time.Now()
					}
					if liveRenderCtrl.shouldRender(now) {
						if enqueueRenderRequest(renderReqCh) {
							liveRenderCtrl.recordRender(now, 0)
						}
						if err := pollRenderError(renderErrCh); err != nil {
							return err
						}
					}
				}
				if opts.progress {
					fmt.Fprintf(stderr, "progress lines=%d\n", lines)
				}
			}
			return nil
		})
	}
	for i, batch := range pendingBatches {
		if batch != nil && len(batch.refs) > 0 {
			if err := sendBatchToWorker(jobChans[i], batch, workerFail); err != nil {
				sendErr = err
				break
			}
			pendingBatches[i] = nil
		} else if batch != nil {
			recycleLineBatch(batch)
			batchPool.Put(batch)
			pendingBatches[i] = nil
		}
	}
	if liveMode {
		close(renderReqCh)
		renderWG.Wait()
		select {
		case renderErr := <-renderErrCh:
			sendErr = renderErr
		default:
		}
	}
	close(stopCh)
	if sampleMemory {
		<-done
	}
	for i := range jobChans {
		close(jobChans[i])
	}
	wg.Wait()
	close(resultCh)
	close(errCh)

	if sendErr != nil {
		return nil, stats, sendErr
	}
	for workerErr := range errCh {
		if workerErr != nil {
			return nil, stats, workerErr
		}
	}

	shardEntries := make([][]entry, 0, workerCount)
	for result := range resultCh {
		shardEntries = append(shardEntries, result.entries)
		stats.unique += uint64(result.unique)
		stats.peakCounterBytes += result.bytes
	}
	stats.lines = lineCounts.Load()
	stats.rssRequested = opts.statsRSS
	stats.peakHeapSysBytes = peakHeapSys.Load()
	if heap := currentHeapSysBytes(); heap > stats.peakHeapSysBytes {
		stats.peakHeapSysBytes = heap
	}
	if opts.statsRSS {
		if rss, ok := platform.ReadMaxRSSBytes(); ok {
			rssAvailable.Store(true)
			setAtomicMax(&peakRSS, rss)
		}
		stats.rssAvailable = rssAvailable.Load()
		stats.peakRSSBytes = peakRSS.Load()
	}
	return shardEntries, stats, nil
}

func enqueueRenderRequest(renderReqCh chan struct{}) bool {
	select {
	case renderReqCh <- struct{}{}:
		return true
	default:
		return false
	}
}

func pollLiveChannels(
	ctx context.Context,
	stopCh <-chan struct{},
	ctrl *liveRenderControl,
	renderDurCh <-chan time.Duration,
	renderErrCh <-chan error,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	select {
	case <-stopCh:
		return errors.New("processing stopped")
	default:
	}
	drainRenderDurations(ctrl, renderDurCh)
	return pollRenderError(renderErrCh)
}

func drainRenderDurations(ctrl *liveRenderControl, renderDurCh <-chan time.Duration) {
	for {
		select {
		case renderDur := <-renderDurCh:
			ctrl.recordRender(time.Now(), renderDur)
		default:
			return
		}
	}
}

func pollRenderError(renderErrCh <-chan error) error {
	select {
	case renderErr := <-renderErrCh:
		return renderErr
	default:
		return nil
	}
}

func snapshotEntriesFromWorkers(
	reqChans []chan snapshotRequest,
	responses []chan []entry,
	shards [][]entry,
	dirty []atomic.Bool,
	opts options,
	mergeBuf []entry,
	nodeBuf []mergeNode,
	requested []bool,
) ([]entry, []mergeNode, []bool) {
	topN, showAll := liveSnapshotSelection(opts)
	if cap(requested) < len(reqChans) {
		requested = make([]bool, len(reqChans))
	}
	requested = requested[:len(reqChans)]
	for i := range requested {
		requested[i] = false
	}
	for i, req := range reqChans {
		if req == nil || responses[i] == nil {
			shards[i] = nil
			continue
		}
		needRefresh := shards[i] == nil || dirty[i].Swap(false)
		if !needRefresh {
			continue
		}
		req <- snapshotRequest{
			topN:    topN,
			showAll: showAll,
			reverse: opts.reverse,
		}
		requested[i] = true
	}
	for i, resp := range responses {
		if resp == nil || !requested[i] {
			continue
		}
		shards[i] = <-resp
	}
	merged, nodes := mergeSortedShardsInto(shards, opts.reverse, topN, showAll || topN < 0, mergeBuf, nodeBuf)
	return merged, nodes, requested
}

func liveSnapshotSelection(opts options) (topN int, showAll bool) {
	if opts.showAll {
		// In live all-output mode, stream a bounded preview during updates to keep
		// snapshot payloads manageable. Final EOF output still uses full exact data.
		return liveAllPreviewTopN, false
	}
	return opts.topN, opts.showAll
}

func streamLines(ctx context.Context, inputs []io.ReadCloser, emit func([]byte) error) error {
	for _, input := range inputs {
		reader := acquireStreamReader(input)
		readErr := func() error {
			var longLine []byte
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				fragment, err := reader.ReadSlice('\n')
				if errors.Is(err, bufio.ErrBufferFull) {
					longLine = append(longLine, fragment...)
					continue
				}

				if len(fragment) > 0 {
					if len(longLine) > 0 {
						longLine = append(longLine, fragment...)
						line := trimLineEnding(longLine)
						if emitErr := emit(line); emitErr != nil {
							return emitErr
						}
						longLine = longLine[:0]
					} else {
						line := trimLineEnding(fragment)
						if emitErr := emit(line); emitErr != nil {
							return emitErr
						}
					}
				} else if len(longLine) > 0 && errors.Is(err, io.EOF) {
					line := trimLineEnding(longLine)
					if emitErr := emit(line); emitErr != nil {
						return emitErr
					}
					longLine = longLine[:0]
				}
				if err == nil {
					continue
				}
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			return nil
		}()
		releaseStreamReader(reader)
		if readErr != nil {
			return readErr
		}
	}
	return nil
}

func streamLinesDispatch(
	ctx context.Context,
	inputs []io.ReadCloser,
	workerCount int,
	batchSize int,
	lineCounts *atomic.Uint64,
	pendingBatches []*lineBatch,
	batchPool *sync.Pool,
	jobChans []chan *lineBatch,
	workerFail *workerFailure,
) error {
	for _, input := range inputs {
		reader := acquireStreamReader(input)
		readErr := func() error {
			var longLine []byte
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				if err := workerFail.Err(); err != nil {
					return err
				}
				fragment, err := reader.ReadSlice('\n')
				if errors.Is(err, bufio.ErrBufferFull) {
					longLine = append(longLine, fragment...)
					continue
				}

				if len(fragment) > 0 {
					var line []byte
					if len(longLine) > 0 {
						longLine = append(longLine, fragment...)
						line = trimLineEnding(longLine)
					} else {
						line = trimLineEnding(fragment)
					}
					lineCounts.Add(1)
					idx := shardIndex(line, workerCount)
					if err := appendLineToPendingBatch(line, idx, pendingBatches, batchPool, jobChans, batchSize, false, workerFail); err != nil {
						return err
					}
					if len(longLine) > 0 {
						longLine = longLine[:0]
					}
				} else if len(longLine) > 0 && errors.Is(err, io.EOF) {
					line := trimLineEnding(longLine)
					lineCounts.Add(1)
					idx := shardIndex(line, workerCount)
					if err := appendLineToPendingBatch(line, idx, pendingBatches, batchPool, jobChans, batchSize, false, workerFail); err != nil {
						return err
					}
					longLine = longLine[:0]
				}
				if err == nil {
					continue
				}
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			return nil
		}()
		releaseStreamReader(reader)
		if readErr != nil {
			return readErr
		}
	}
	return nil
}

func appendLineToPendingBatch(
	line []byte,
	shard int,
	pendingBatches []*lineBatch,
	batchPool *sync.Pool,
	jobChans []chan *lineBatch,
	batchSize int,
	liveMode bool,
	workerFail *workerFailure,
) error {
	batch := pendingBatches[shard]
	if batch == nil {
		batch = batchPool.Get().(*lineBatch)
		recycleLineBatch(batch)
		pendingBatches[shard] = batch
	}
	start := len(batch.slab)
	batch.slab = append(batch.slab, line...)
	batch.refs = append(batch.refs, lineRef{start: start, end: len(batch.slab)})
	if shouldFlushBatchAdaptive(batch, batchSize, jobChans[shard], liveMode) {
		if err := sendBatchToWorker(jobChans[shard], batch, workerFail); err != nil {
			return err
		}
		pendingBatches[shard] = nil
	}
	return nil
}

func flushPendingBatches(
	pendingBatches []*lineBatch,
	batchPool *sync.Pool,
	jobChans []chan *lineBatch,
	workerFail *workerFailure,
) error {
	for i, batch := range pendingBatches {
		if batch == nil {
			continue
		}
		if len(batch.refs) == 0 {
			recycleLineBatch(batch)
			batchPool.Put(batch)
			pendingBatches[i] = nil
			continue
		}
		if err := sendBatchToWorker(jobChans[i], batch, workerFail); err != nil {
			return err
		}
		pendingBatches[i] = nil
	}
	return nil
}

func processBatchIntoCounter(
	c *mapCounter,
	tracker *liveTopNTracker,
	batch *lineBatch,
	growthState *counterGrowthState,
	memLimit uint64,
) {
	for _, ref := range batch.refs {
		line := batch.slab[ref.start:ref.end]
		if tracker != nil {
			count, insertedKey, inserted := c.IncTracked(line)
			tracker.UpdateLine(line, count, insertedKey, inserted)
			continue
		}
		c.Inc(line)
	}
	maybeGrowCounter(c, growthState, len(batch.refs), memLimit)
}

func checkMemoryLimit(c *mapCounter, memLimit uint64, memCheckCountdown *int) error {
	if memLimit == 0 {
		return nil
	}
	*memCheckCountdown = *memCheckCountdown - 1
	if *memCheckCountdown > 0 {
		return nil
	}
	*memCheckCountdown = memCheckEveryBatches
	if c.EstimatedBytes() > memLimit {
		return errors.New("memory limit exceeded; spill-to-disk mode is not implemented")
	}
	return nil
}

func finalMemoryLimitCheck(c *mapCounter, memLimit uint64) error {
	if memLimit == 0 {
		return nil
	}
	if c.EstimatedBytes() > memLimit {
		return errors.New("memory limit exceeded; spill-to-disk mode is not implemented")
	}
	return nil
}

type workerFailure struct {
	mu   sync.Mutex
	err  error
	done chan struct{}
	once sync.Once
}

func newWorkerFailure() *workerFailure {
	return &workerFailure{done: make(chan struct{})}
}

func (w *workerFailure) Fail(err error) {
	if err == nil {
		return
	}
	w.once.Do(func() {
		w.mu.Lock()
		w.err = err
		w.mu.Unlock()
		close(w.done)
	})
}

func (w *workerFailure) Err() error {
	select {
	case <-w.done:
	default:
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	return errors.New("worker failed")
}

func sendBatchToWorker(jobChan chan *lineBatch, batch *lineBatch, workerFail *workerFailure) error {
	if workerFail == nil {
		jobChan <- batch
		return nil
	}
	select {
	case jobChan <- batch:
		return nil
	case <-workerFail.done:
		return workerFail.Err()
	}
}

var streamReaderPool sync.Pool

var streamReaderResetTarget = eofReader{}

type eofReader struct{}

func (eofReader) Read([]byte) (int, error) {
	return 0, io.EOF
}

func acquireStreamReader(r io.Reader) *bufio.Reader {
	if pooled := streamReaderPool.Get(); pooled != nil {
		reader := pooled.(*bufio.Reader)
		reader.Reset(r)
		return reader
	}
	return bufio.NewReaderSize(r, 256*1024)
}

func releaseStreamReader(reader *bufio.Reader) {
	if reader == nil {
		return
	}
	reader.Reset(streamReaderResetTarget)
	streamReaderPool.Put(reader)
}

func recycleLineBatch(batch *lineBatch) {
	if cap(batch.slab) > 1<<20 {
		batch.slab = make([]byte, 0, maxBatchBytes)
	} else {
		batch.slab = batch.slab[:0]
	}
	if cap(batch.refs) > 1<<16 {
		batch.refs = make([]lineRef, 0, 256)
	} else {
		batch.refs = batch.refs[:0]
	}
}

func trimLineEnding(line []byte) []byte {
	n := len(line)
	if n == 0 {
		return line
	}
	if line[n-1] == '\n' {
		n--
	}
	if n > 0 && line[n-1] == '\r' {
		n--
	}
	return line[:n]
}

func sortEntries(entries []entry, reverse bool) {
	sort.Slice(entries, func(i, j int) bool {
		return entryBetter(entries[i], entries[j], reverse)
	})
}

func entryBetter(a, b entry, reverse bool) bool {
	if a.count == b.count {
		return a.value < b.value
	}
	if reverse {
		return a.count < b.count
	}
	return a.count > b.count
}

func entryWorse(a, b entry, reverse bool) bool {
	return entryBetter(b, a, reverse)
}

type topNHeap struct {
	reverse bool
	items   []entry
}

func (h topNHeap) Len() int { return len(h.items) }

func (h topNHeap) Less(i, j int) bool {
	return entryWorse(h.items[i], h.items[j], h.reverse)
}

func (h topNHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *topNHeap) Push(x any) {
	h.items = append(h.items, x.(entry))
}

func (h *topNHeap) Pop() any {
	n := len(h.items)
	item := h.items[n-1]
	h.items = h.items[:n-1]
	return item
}

func selectTopN(entries []entry, n int, reverse bool) []entry {
	if n <= 0 || len(entries) == 0 {
		return nil
	}
	if n >= len(entries) {
		return entries
	}
	h := &topNHeap{
		reverse: reverse,
		items:   make([]entry, 0, n),
	}
	heap.Init(h)
	for _, e := range entries {
		if h.Len() < n {
			heap.Push(h, e)
			continue
		}
		if entryBetter(e, h.items[0], reverse) {
			heap.Pop(h)
			heap.Push(h, e)
		}
	}
	return h.items
}

func finalizeEntries(entries []entry, opts options) []entry {
	if opts.showAll || opts.topN < 0 || opts.topN >= len(entries) {
		sortEntries(entries, opts.reverse)
		return entries
	}
	selected := selectTopN(entries, opts.topN, opts.reverse)
	sortEntries(selected, opts.reverse)
	return selected
}

type mergeNode struct {
	shard int
	index int
	entry entry
}

type mergeHeap struct {
	reverse bool
	items   []mergeNode
}

func (h mergeHeap) Len() int { return len(h.items) }

func (h mergeHeap) Less(i, j int) bool {
	return entryBetter(h.items[i].entry, h.items[j].entry, h.reverse)
}

func (h mergeHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *mergeHeap) Push(x any) {
	h.items = append(h.items, x.(mergeNode))
}

func (h *mergeHeap) Pop() any {
	n := len(h.items)
	item := h.items[n-1]
	h.items = h.items[:n-1]
	return item
}

func finalizeFromShards(shards [][]entry, opts options) []entry {
	limit := opts.topN
	allMode := opts.showAll || limit < 0

	for i := range shards {
		if len(shards[i]) == 0 {
			continue
		}
		if !allMode && limit < len(shards[i]) {
			shards[i] = selectTopN(shards[i], limit, opts.reverse)
		}
		sortEntries(shards[i], opts.reverse)
	}
	return mergeSortedShards(shards, opts.reverse, limit, allMode)
}

func mergeSortedShards(shards [][]entry, reverse bool, limit int, allMode bool) []entry {
	out, _ := mergeSortedShardsInto(shards, reverse, limit, allMode, nil, nil)
	return out
}

func mergeSortedShardsInto(shards [][]entry, reverse bool, limit int, allMode bool, dst []entry, nodeBuf []mergeNode) ([]entry, []mergeNode) {
	nodes := nodeBuf[:0]
	if cap(nodes) < len(shards) {
		nodes = make([]mergeNode, 0, len(shards))
	}
	h := &mergeHeap{
		reverse: reverse,
		items:   nodes,
	}
	for i := range shards {
		if len(shards[i]) == 0 {
			continue
		}
		heap.Push(h, mergeNode{shard: i, index: 0, entry: shards[i][0]})
	}
	if h.Len() == 0 {
		return dst[:0], h.items[:0]
	}

	out := dst[:0]
	if allMode {
		total := 0
		for i := range shards {
			total += len(shards[i])
		}
		if cap(out) < total {
			out = make([]entry, 0, total)
		}
	} else {
		if limit <= 0 {
			return out, h.items[:0]
		}
		if cap(out) < limit {
			out = make([]entry, 0, limit)
		}
	}

	for h.Len() > 0 {
		node := heap.Pop(h).(mergeNode)
		out = append(out, node.entry)
		if !allMode && len(out) >= limit {
			break
		}
		nextIndex := node.index + 1
		if nextIndex < len(shards[node.shard]) {
			heap.Push(h, mergeNode{
				shard: node.shard,
				index: nextIndex,
				entry: shards[node.shard][nextIndex],
			})
		}
	}
	return out, h.items[:0]
}

func (r *liveRenderer) render(entries []entry) error {
	bw := r.bw
	if !r.initialized {
		if _, err := bw.WriteString("\x1b[2J\x1b[H"); err != nil {
			return err
		}
		for _, e := range entries {
			if err := writeRenderedEntry(bw, e, r.showCount); err != nil {
				return err
			}
		}
		r.prevEntries = append(r.prevEntries[:0], entries...)
		r.initialized = true
		return bw.Flush()
	}

	maxLines := len(entries)
	if len(r.prevEntries) > maxLines {
		maxLines = len(r.prevEntries)
	}

	bitmapWords := (maxLines + 63) / 64
	if cap(r.dirtyBitmap) < bitmapWords {
		r.dirtyBitmap = make([]uint64, bitmapWords)
	}
	r.dirtyBitmap = r.dirtyBitmap[:bitmapWords]
	for i := range r.dirtyBitmap {
		r.dirtyBitmap[i] = 0
	}

	changed := false
	for i := 0; i < maxLines; i++ {
		var prev entry
		prevOK := false
		if i < len(r.prevEntries) {
			prev = r.prevEntries[i]
			prevOK = true
		}
		var curr entry
		currOK := false
		if i < len(entries) {
			curr = entries[i]
			currOK = true
		}
		lineChanged := prevOK != currOK || (currOK && (prev.count != curr.count || prev.value != curr.value))
		if !lineChanged {
			continue
		}
		changed = true
		word := i >> 6
		bit := uint(i & 63)
		r.dirtyBitmap[word] |= 1 << bit
	}
	if !changed {
		return nil
	}

	for i := 0; i < maxLines; {
		if !bitmapBitSet(r.dirtyBitmap, i) {
			i++
			continue
		}
		runStart := i
		i++
		for i < maxLines && bitmapBitSet(r.dirtyBitmap, i) {
			i++
		}
		runEnd := i
		r.frameBuf = r.frameBuf[:0]
		r.frameBuf = appendCursorHomeLine(r.frameBuf, runStart+1)
		for row := runStart; row < runEnd; row++ {
			r.frameBuf = append(r.frameBuf, '\x1b', '[', '2', 'K')
			if row < len(entries) {
				r.frameBuf = appendRenderedEntryInlineBytes(r.frameBuf, entries[row], r.showCount)
			}
			if row+1 < runEnd {
				r.frameBuf = append(r.frameBuf, '\n')
			}
		}
		if _, err := bw.Write(r.frameBuf); err != nil {
			return err
		}
	}

	if err := writeCursorHomeLine(bw, len(entries)+1, r.ansiScratch[:0]); err != nil {
		return err
	}
	r.prevEntries = append(r.prevEntries[:0], entries...)
	return bw.Flush()
}

func (r *liveRenderer) matches(entries []entry) bool {
	if !r.initialized {
		return false
	}
	if len(r.prevEntries) != len(entries) {
		return false
	}
	for i := range entries {
		if r.prevEntries[i].count != entries[i].count || r.prevEntries[i].value != entries[i].value {
			return false
		}
	}
	return true
}

func writeRenderedEntry(w io.Writer, e entry, showCount bool) error {
	if err := writeRenderedEntryInline(w, e, showCount); err != nil {
		return err
	}
	_, err := io.WriteString(w, "\n")
	return err
}

func writeRenderedEntryInline(w io.Writer, e entry, showCount bool) error {
	if showCount {
		var scratch [24]byte
		buf := strconv.AppendUint(scratch[:0], e.count, 10)
		buf = append(buf, ' ')
		if _, err := w.Write(buf); err != nil {
			return err
		}
		_, err := io.WriteString(w, e.value)
		return err
	}
	_, err := io.WriteString(w, e.value)
	return err
}

func appendRenderedEntryInlineBytes(dst []byte, e entry, showCount bool) []byte {
	if showCount {
		dst = strconv.AppendUint(dst, e.count, 10)
		dst = append(dst, ' ')
	}
	return append(dst, e.value...)
}

func writeCursorClearLine(w io.Writer, line int, buf []byte) error {
	buf = append(buf, "\x1b["...)
	buf = strconv.AppendInt(buf, int64(line), 10)
	buf = append(buf, ';', '1', 'H')
	buf = append(buf, "\x1b[2K"...)
	_, err := w.Write(buf)
	return err
}

func writeCursorHomeLine(w io.Writer, line int, buf []byte) error {
	buf = append(buf, "\x1b["...)
	buf = strconv.AppendInt(buf, int64(line), 10)
	buf = append(buf, ';', '1', 'H')
	_, err := w.Write(buf)
	return err
}

func appendCursorHomeLine(buf []byte, line int) []byte {
	buf = append(buf, "\x1b["...)
	buf = strconv.AppendInt(buf, int64(line), 10)
	return append(buf, ';', '1', 'H')
}

func bitmapBitSet(words []uint64, i int) bool {
	word := i >> 6
	if word >= len(words) {
		return false
	}
	mask := uint64(1) << uint(i&63)
	return words[word]&mask != 0
}

func writeOutput(w io.Writer, entries []entry, opts options) error {
	rows := make([]output.Row, 0, len(entries))
	for _, e := range entries {
		rows = append(rows, output.Row{Value: e.value, Count: e.count})
	}
	writer := output.NewWriter(w, opts.output, opts.showCount)
	return writer.Write(rows)
}

func writeStats(w io.Writer, stats runStats) {
	fmt.Fprintf(w, "lines_processed: %d\n", stats.lines)
	fmt.Fprintf(w, "unique_values: %d\n", stats.unique)
	fmt.Fprintf(w, "duplicates: %d\n", stats.duplicates)
	fmt.Fprintf(w, "elapsed: %s\n", stats.elapsed)
	fmt.Fprintf(w, "throughput_lines_per_sec: %.2f\n", stats.throughput)
	fmt.Fprintf(w, "peak_counter_estimate_bytes: %d\n", stats.peakCounterBytes)
	fmt.Fprintf(w, "peak_heap_sys_bytes: %d\n", stats.peakHeapSysBytes)
	if stats.rssRequested {
		if stats.rssAvailable {
			fmt.Fprintf(w, "peak_rss_bytes: %d\n", stats.peakRSSBytes)
		} else {
			fmt.Fprintf(w, "peak_rss_bytes: unavailable\n")
		}
	}
}

func currentHeapSysBytes() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Sys
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func setAtomicMax(target *atomic.Uint64, value uint64) {
	for {
		current := target.Load()
		if value <= current {
			return
		}
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func durationFromSeconds(seconds float64) time.Duration {
	if seconds <= 0 {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}

func shouldEmitUpdate(lines uint64, now time.Time, nextLineUpdate *uint64, lastUpdate *time.Time, lineEvery uint64, timeEvery time.Duration) bool {
	emit := false
	if shouldEmitLineUpdate(lines, nextLineUpdate, lineEvery) {
		emit = true
	}
	if timeEvery > 0 && now.Sub(*lastUpdate) >= timeEvery {
		emit = true
	}
	if emit {
		*lastUpdate = now
	}
	return emit
}

func shouldEmitLineUpdate(lines uint64, nextLineUpdate *uint64, lineEvery uint64) bool {
	if lineEvery == 0 || lines < *nextLineUpdate {
		return false
	}
	for *nextLineUpdate <= lines {
		*nextLineUpdate += lineEvery
	}
	return true
}

func computeBatchLineTarget(workers int, liveMode bool) int {
	if liveMode {
		return 64
	}
	if workers >= 8 {
		return 256
	}
	if workers >= 4 {
		return 192
	}
	return 128
}

type counterGrowthState struct {
	linesSeen uint64
	done      bool
}

func initialCounterCapacity(opts options, workers int, liveMode bool) int {
	capacity := 1024
	if !liveMode {
		capacity = 2048
	}
	if workers <= 2 {
		capacity *= 2
	}
	if opts.showAll || opts.topN < 0 {
		capacity *= 2
	}
	if opts.topN > 0 && opts.topN*2 > capacity {
		capacity = opts.topN * 2
	}
	if capacity < 1024 {
		capacity = 1024
	}
	if capacity > 65536 {
		capacity = 65536
	}
	return capacity
}

func maybeGrowCounter(c *mapCounter, state *counterGrowthState, batchLines int, memLimit uint64) {
	if state.done || batchLines <= 0 {
		return
	}
	state.linesSeen += uint64(batchLines)
	if state.linesSeen < counterGrowthProbeLines {
		return
	}
	state.done = true
	unique := c.Len()
	if unique < counterGrowthMinUnique {
		return
	}
	if float64(unique)/float64(state.linesSeen) < counterGrowthMinUniqueRatio {
		return
	}
	target := unique * counterGrowthTargetFactor
	if target < 65536 {
		target = 65536
	}
	if memLimit > 0 {
		estimatedAfterGrowth := c.EstimatedBytes() * 2
		if estimatedAfterGrowth > memLimit {
			return
		}
	}
	c.Grow(target)
}

func shouldFlushBatchAdaptive(batch *lineBatch, baseLineTarget int, jobChan chan *lineBatch, liveMode bool) bool {
	lineTarget, byteTarget := adaptiveBatchTargets(baseLineTarget, len(jobChan), cap(jobChan), liveMode)
	return len(batch.refs) >= lineTarget || len(batch.slab) >= byteTarget
}

func adaptiveBatchTargets(baseLineTarget, queueLen, queueCap int, liveMode bool) (lineTarget int, byteTarget int) {
	factor := 1
	if queueCap > 0 {
		// Keep bigger in-flight batches when worker queues are mostly empty to
		// reduce channel send/schedule churn in the ingest hot path.
		switch {
		case queueLen*8 <= queueCap:
			factor = maxAdaptiveBatchFactor
		case queueLen*3 <= queueCap:
			factor = 2
		}
	}
	if liveMode && factor > 2 {
		factor = 2
	}

	lineTarget = baseLineTarget * factor
	byteTarget = maxBatchBytes * factor
	return lineTarget, byteTarget
}

func shardIndex(line []byte, workers int) int {
	if workers <= 1 {
		return 0
	}
	return int(crc32.Checksum(line, crc32cTable) % uint32(workers))
}

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func effectiveLiveRenderInterval(opts options, progressInterval time.Duration) time.Duration {
	if !(opts.flushEvery > 0 && opts.showAll) {
		return 0
	}
	if progressInterval == 0 || progressInterval < liveAllMinRenderInterval {
		return liveAllMinRenderInterval
	}
	return progressInterval
}
