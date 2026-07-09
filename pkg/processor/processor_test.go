package processor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRunEmptyInput(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code := run([]string{}, strings.NewReader(""), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
	if out.String() != "" {
		t.Fatalf("expected empty output, got %q", out.String())
	}
}

// TestLiveModeEmptyInput ensures that live mode on an empty stream produces no
// stdout output (no ANSI sequences, no status bar).
func TestLiveModeEmptyInput(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code := run([]string{"-u", "1"}, strings.NewReader(""), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
	if out.Len() != 0 {
		t.Fatalf("expected no stdout on empty live-mode input, got %d bytes: %q", out.Len(), out.String())
	}
}

// TestNoStatusFlagSuppressesStatusBar checks that --no-status omits the status
// bar text from live mode output.
func TestNoStatusFlagSuppressesStatusBar(t *testing.T) {
	input := strings.Repeat("foo\nbar\nbaz\n", 100)
	var with, without, errOut bytes.Buffer

	if run([]string{"-n", "3", "-u", "50"}, strings.NewReader(input), &with, &errOut) != 0 {
		t.Fatalf("run with status failed: %s", errOut.String())
	}
	errOut.Reset()
	if run([]string{"-n", "3", "-u", "50", "--no-status"}, strings.NewReader(input), &without, &errOut) != 0 {
		t.Fatalf("run without status failed: %s", errOut.String())
	}

	if !strings.Contains(with.String(), "streaming") && !strings.Contains(with.String(), "complete") {
		t.Fatal("expected status bar text (streaming/complete) in default live output")
	}
	if strings.Contains(without.String(), "streaming") || strings.Contains(without.String(), "complete") {
		t.Fatalf("expected no status bar text with --no-status, got %q", without.String())
	}
}

// TestStatusFlagOverridesConfig verifies that --status and --no-status are
// recognized by the flag parser without error.
func TestStatusFlagOverridesConfig(t *testing.T) {
	input := "a\nb\na\n"
	var out, errOut bytes.Buffer
	if run([]string{"-u", "1", "--status"}, strings.NewReader(input), &out, &errOut) != 0 {
		t.Fatalf("--status flag rejected: %s", errOut.String())
	}
	out.Reset()
	errOut.Reset()
	if run([]string{"-u", "1", "--no-status"}, strings.NewReader(input), &out, &errOut) != 0 {
		t.Fatalf("--no-status flag rejected: %s", errOut.String())
	}
}

func TestRunDuplicateLines(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	input := "apple\nbanana\napple\napple\nbanana\n"
	code := run([]string{}, strings.NewReader(input), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
	got := out.String()
	want := "3 apple\n2 banana\n"
	if got != want {
		t.Fatalf("unexpected output:\nwant:\n%s\ngot:\n%s", want, got)
	}
}

func TestRunAllUnique(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	input := "z\nb\na\n"
	code := run([]string{}, strings.NewReader(input), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
	if out.String() != "1 a\n1 b\n1 z\n" {
		t.Fatalf("unexpected deterministic ordering: %q", out.String())
	}
}

func TestRunUTF8AndBinary(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	data := []byte("café\ncaf\xc3\xa9\nbin\x00ary\nbin\x00ary\n")
	code := run([]string{}, bytes.NewReader(data), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
	got := out.Bytes()
	if !bytes.Contains(got, []byte("2 bin\x00ary\n")) {
		t.Fatalf("expected binary key count in output, got %q", string(got))
	}
}

func TestRunMultipleFilesAsOneStream(t *testing.T) {
	dir := t.TempDir()
	fileA := filepath.Join(dir, "a.txt")
	fileB := filepath.Join(dir, "b.txt")
	if err := os.WriteFile(fileA, []byte("x\ny\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fileB, []byte("x\nz\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	var errOut bytes.Buffer
	code := run([]string{fileA, fileB}, strings.NewReader(""), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
	want := "2 x\n1 y\n1 z\n"
	if out.String() != want {
		t.Fatalf("unexpected output:\nwant:\n%s\ngot:\n%s", want, out.String())
	}
}

func TestRunLongLines(t *testing.T) {
	long := strings.Repeat("a", 2*1024*1024)
	var out bytes.Buffer
	var errOut bytes.Buffer
	code := run([]string{}, strings.NewReader(long+"\n"+long+"\n"), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
	if !strings.HasPrefix(out.String(), "2 ") {
		t.Fatalf("unexpected output for long lines: %q", out.String()[:8])
	}
}

func TestRunDeterministicOrdering(t *testing.T) {
	input := "b\na\nc\nb\na\nc\n"
	var firstOut bytes.Buffer
	var secondOut bytes.Buffer
	var errOut bytes.Buffer
	code := run([]string{}, strings.NewReader(input), &firstOut, &errOut)
	if code != 0 {
		t.Fatalf("first run failed: %s", errOut.String())
	}
	errOut.Reset()
	code = run([]string{}, strings.NewReader(input), &secondOut, &errOut)
	if code != 0 {
		t.Fatalf("second run failed: %s", errOut.String())
	}
	if firstOut.String() != secondOut.String() {
		t.Fatalf("non-deterministic output:\n%s\n!=\n%s", firstOut.String(), secondOut.String())
	}
}

func TestRunTopN(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	input := "a\na\na\nb\nb\nc\n"
	code := run([]string{"-n", "2"}, strings.NewReader(input), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
	if out.String() != "3 a\n2 b\n" {
		t.Fatalf("unexpected top-N output: %q", out.String())
	}
}

func TestRunReverseOrdering(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	input := "a\na\na\nb\nb\nc\n"
	code := run([]string{"-r"}, strings.NewReader(input), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
	if out.String() != "1 c\n2 b\n3 a\n" {
		t.Fatalf("unexpected reverse output: %q", out.String())
	}
}

func TestRunNoCountFlag(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	input := "a\na\na\nb\nb\nc\n"
	code := run([]string{"--no-count"}, strings.NewReader(input), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
	if out.String() != "a\nb\nc\n" {
		t.Fatalf("unexpected output without counts: %q", out.String())
	}
}

func TestRunTopNReverseOrdering(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	input := "a\na\na\nb\nb\nc\nd\n"
	code := run([]string{"-r", "-n", "2"}, strings.NewReader(input), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
	if out.String() != "1 c\n1 d\n" {
		t.Fatalf("unexpected reverse top-N output: %q", out.String())
	}
}

func TestRunLiveRefresh(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	input := "a\na\nb\n"
	code := run([]string{"-u", "1", "-n", "2"}, strings.NewReader(input), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
	got := out.String()
	if !strings.Contains(got, "\x1b[2J\x1b[H") {
		t.Fatalf("expected live clear sequence in output, got: %q", got)
	}
	if !strings.Contains(got, "2 a") || !strings.Contains(got, "1 b") {
		t.Fatalf("expected final leaderboard in live output, got: %q", got)
	}
}

func TestRunLiveRefreshLongFlag(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	input := "a\na\nb\n"
	code := run([]string{"--update-every", "1", "-n", "2"}, strings.NewReader(input), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
	got := out.String()
	if !strings.Contains(got, "\x1b[2J\x1b[H") {
		t.Fatalf("expected live clear sequence in output, got: %q", got)
	}
	if !strings.Contains(got, "2 a") || !strings.Contains(got, "1 b") {
		t.Fatalf("expected final leaderboard in live output, got: %q", got)
	}
}

func TestRunLiveRefreshDeprecatedAliasStillWorks(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code := run([]string{"-f", "1"}, strings.NewReader("a\n"), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
}

func TestRunLiveRefreshEmitsCountsBeforeEOF(t *testing.T) {
	inR, inW := io.Pipe()
	var out lockedBuffer
	var errOut lockedBuffer

	done := make(chan int, 1)
	go func() {
		done <- run([]string{"-u", "1", "--workers", "8", "-n", "5"}, inR, &out, &errOut)
	}()

	if _, err := io.WriteString(inW, "a\n"); err != nil {
		t.Fatalf("failed to write input: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		if strings.Contains(out.String(), "1 a") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected live output before EOF, got output=%q stderr=%q", out.String(), errOut.String())
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := inW.Close(); err != nil {
		t.Fatalf("failed to close input: %v", err)
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("run did not finish")
	}
}

func TestRunLiveRefreshRejectsJSON(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code := run([]string{"-u", "1", "--json"}, strings.NewReader("a\n"), &out, &errOut)
	if code != 2 {
		t.Fatalf("expected exit code 2, got %d", code)
	}
	if !strings.Contains(errOut.String(), "-u/--update-every is only supported with plain output") {
		t.Fatalf("unexpected error output: %s", errOut.String())
	}
}

func TestRunLiveRefreshUsesSameCadenceAsProgress(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	input := "a\nb\nc\n"
	code := run([]string{"-u", "2", "--progress", "--progress-every", "1"}, strings.NewReader(input), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
	progress := errOut.String()
	if !strings.Contains(progress, "progress lines=2") {
		t.Fatalf("expected progress at linked cadence, got: %q", progress)
	}
	if strings.Contains(progress, "progress lines=1") {
		t.Fatalf("expected no progress at 1-line cadence in live mode, got: %q", progress)
	}
}

func TestRunAllowsSecondsCadenceOnly(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code := run([]string{"--progress", "--progress-every", "0", "--progress-every-seconds", "1"}, strings.NewReader("a\n"), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
}

func TestRunAllowsShortSecondsCadenceFlag(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code := run([]string{"--progress", "--progress-every", "0", "-s", "1"}, strings.NewReader("a\n"), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
}

func TestRunRejectsMissingCadence(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code := run([]string{"--progress-every", "0"}, strings.NewReader("a\n"), &out, &errOut)
	if code != 2 {
		t.Fatalf("expected exit code 2, got %d", code)
	}
	if !strings.Contains(errOut.String(), "progress-every or progress-every-seconds") {
		t.Fatalf("unexpected error output: %s", errOut.String())
	}
}

func TestEffectiveLiveRenderInterval(t *testing.T) {
	base := 50 * time.Millisecond
	opts := options{flushEvery: 1, showAll: true}
	if got := effectiveLiveRenderInterval(opts, base); got != liveAllMinRenderInterval {
		t.Fatalf("expected all-live interval floor %s, got %s", liveAllMinRenderInterval, got)
	}
	if got := effectiveLiveRenderInterval(opts, 2*time.Second); got != 2*time.Second {
		t.Fatalf("expected configured interval to be preserved, got %s", got)
	}
	if got := effectiveLiveRenderInterval(options{flushEvery: 1, showAll: false}, base); got != 0 {
		t.Fatalf("expected no throttle outside live all mode, got %s", got)
	}
}

func TestLiveRenderControlAdaptiveBackoff(t *testing.T) {
	ctrl := newLiveRenderControl(10 * time.Millisecond)
	start := time.Now()
	if !ctrl.shouldRender(start) {
		t.Fatalf("expected initial render to be allowed")
	}
	ctrl.recordRender(start, 100*time.Millisecond)
	if ctrl.currentInterval() < 100*time.Millisecond {
		t.Fatalf("expected adaptive interval to increase after expensive render, got %s", ctrl.currentInterval())
	}
	if ctrl.shouldRender(start.Add(20 * time.Millisecond)) {
		t.Fatalf("expected early redraw to be deferred")
	}
	if !ctrl.shouldRender(start.Add(ctrl.currentInterval())) {
		t.Fatalf("expected redraw at adaptive interval boundary")
	}
}

func TestRunCSVAndJSON(t *testing.T) {
	input := "orange\napple\norange\n"
	t.Run("csv", func(t *testing.T) {
		var out bytes.Buffer
		var errOut bytes.Buffer
		code := run([]string{"--csv"}, strings.NewReader(input), &out, &errOut)
		if code != 0 {
			t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
		}
		want := "count,value\n2,orange\n1,apple\n"
		if out.String() != want {
			t.Fatalf("unexpected csv output:\nwant:\n%s\ngot:\n%s", want, out.String())
		}
	})
	t.Run("json", func(t *testing.T) {
		var out bytes.Buffer
		var errOut bytes.Buffer
		code := run([]string{"--json"}, strings.NewReader(input), &out, &errOut)
		if code != 0 {
			t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
		}
		got := out.String()
		if !strings.Contains(got, `"value":"orange"`) || !strings.Contains(got, `"count":2`) {
			t.Fatalf("unexpected json output: %s", got)
		}
	})
}

func TestRunContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	var errOut bytes.Buffer
	code := RunContext(ctx, []string{}, strings.NewReader("a\n"), &out, &errOut)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	if !strings.Contains(errOut.String(), "context canceled") {
		t.Fatalf("unexpected error output: %s", errOut.String())
	}
}

func TestRunStats(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code := run([]string{"--stats"}, strings.NewReader("a\na\nb\n"), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
	stats := errOut.String()
	if !strings.Contains(stats, "lines_processed: 3") {
		t.Fatalf("missing stats output: %s", stats)
	}
	if !strings.Contains(stats, "unique_values: 2") {
		t.Fatalf("missing unique stats: %s", stats)
	}
}

func TestRunStatsRSS(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	code := run([]string{"--stats", "--stats-rss"}, strings.NewReader("a\na\nb\n"), &out, &errOut)
	if code != 0 {
		t.Fatalf("expected exit code 0, got %d: %s", code, errOut.String())
	}
	stats := errOut.String()
	if !strings.Contains(stats, "peak_rss_bytes:") {
		t.Fatalf("missing rss stats output: %s", stats)
	}
}

func TestRunMemoryLimitEnforcedWithManyWorkers(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	input := testUniqueInput(50_000)
	code := run([]string{"--workers", "8", "--memory-limit-bytes", "1"}, strings.NewReader(input), &out, &errOut)
	if code != 1 {
		t.Fatalf("expected exit code 1, got %d: %s", code, errOut.String())
	}
	if !strings.Contains(errOut.String(), "memory limit exceeded") {
		t.Fatalf("expected memory limit error, got: %s", errOut.String())
	}
}

func TestRunLiveModeMemoryLimitReturnsWithoutHang(t *testing.T) {
	var out bytes.Buffer
	var errOut bytes.Buffer
	input := testUniqueInput(100_000)
	done := make(chan int, 1)
	go func() {
		done <- run([]string{"--workers", "8", "-u", "1000000", "--memory-limit-bytes", "1"}, strings.NewReader(input), &out, &errOut)
	}()
	select {
	case code := <-done:
		if code != 1 {
			t.Fatalf("expected exit code 1, got %d: %s", code, errOut.String())
		}
		if !strings.Contains(errOut.String(), "memory limit exceeded") {
			t.Fatalf("expected memory limit error, got: %s", errOut.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("live mode memory-limit run hung")
	}
}

func TestProcessRandomizedDeterministic(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(&b, "k%03d\n", i%37)
	}
	input := b.String()
	var out1 bytes.Buffer
	var out2 bytes.Buffer
	var errOut bytes.Buffer
	if run([]string{"--workers", "4"}, strings.NewReader(input), &out1, &errOut) != 0 {
		t.Fatalf("first randomized run failed: %s", errOut.String())
	}
	errOut.Reset()
	if run([]string{"--workers", "4"}, strings.NewReader(input), &out2, &errOut) != 0 {
		t.Fatalf("second randomized run failed: %s", errOut.String())
	}
	if out1.String() != out2.String() {
		t.Fatalf("randomized input produced non-deterministic output")
	}
}

func testUniqueInput(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "k%08d\n", i)
	}
	return b.String()
}

func benchmarkInput(size int) string {
	var b strings.Builder
	b.Grow(size * 12)
	for i := 0; i < size; i++ {
		fmt.Fprintf(&b, "line-%06d\n", i%1000)
	}
	return b.String()
}

func benchmarkInputUnique(size int) string {
	var b strings.Builder
	b.Grow(size * 12)
	for i := 0; i < size; i++ {
		fmt.Fprintf(&b, "line-%06d\n", i)
	}
	return b.String()
}

func benchmarkInputZipf(total int, imax uint64, s, v float64) string {
	r := rand.New(rand.NewSource(42))
	z := rand.NewZipf(r, s, v, imax)
	var b strings.Builder
	b.Grow(total * 12)
	for i := 0; i < total; i++ {
		fmt.Fprintf(&b, "line-%06d\n", z.Uint64())
	}
	return b.String()
}

func runBenchmarkCase(b *testing.B, args []string, data string) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var out bytes.Buffer
		var errOut bytes.Buffer
		if code := run(args, strings.NewReader(data), &out, &errOut); code != 0 {
			b.Fatalf("run failed: %s", errOut.String())
		}
	}
}

func BenchmarkRunDuplicateHeavy(b *testing.B) {
	data := benchmarkInput(200_000)
	args := []string{"-n", "50"}
	runBenchmarkCase(b, args, data)
}

func BenchmarkRunHighCardinality(b *testing.B) {
	data := benchmarkInputUnique(200_000)
	args := []string{"-n", "50"}
	runBenchmarkCase(b, args, data)
}

func BenchmarkRunZipfSkewed(b *testing.B) {
	data := benchmarkInputZipf(200_000, 50_000, 1.07, 1.0)
	args := []string{"-n", "50"}
	runBenchmarkCase(b, args, data)
}

func BenchmarkLiveModeCadence(b *testing.B) {
	// Keep this benchmark calibrated so every subcase gets enough samples on
	// typical developer machines, including workers=1 live mode.
	const (
		liveBenchLines = 20_000
		liveBenchIMax  = 10_000
	)
	data := benchmarkInputZipf(liveBenchLines, liveBenchIMax, 1.07, 1.0)
	cases := []struct {
		name string
		args []string
	}{
		{
			name: "non-live-workers8",
			args: []string{"-n", "50", "--workers", "8"},
		},
		{
			name: "live-sparse-workers8",
			args: []string{"-n", "50", "--workers", "8", "-u", "400000"},
		},
		{
			name: "live-frequent-workers8",
			args: []string{"-n", "50", "--workers", "8", "-u", "500"},
		},
		{
			name: "live-frequent-workers1",
			args: []string{"-n", "50", "--workers", "1", "-u", "500"},
		},
		{
			name: "live-all-workers8",
			args: []string{"-a", "--workers", "8", "-u", "500"},
		},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			runBenchmarkCase(b, c.args, data)
		})
	}
}

func BenchmarkCompareUnixPipeline(b *testing.B) {
	if runtime.GOOS == "windows" {
		b.Skip("Unix pipeline baseline is not available on Windows")
	}
	required := []string{"sh", "sort", "uniq"}
	for _, bin := range required {
		if _, err := exec.LookPath(bin); err != nil {
			b.Skipf("missing required binary %q: %v", bin, err)
		}
	}

	data := benchmarkInputZipf(120_000, 30_000, 1.07, 1.0)
	inputFile := filepath.Join(b.TempDir(), "dataset.txt")
	if err := os.WriteFile(inputFile, []byte(data), 0o644); err != nil {
		b.Fatalf("failed writing benchmark input: %v", err)
	}

	b.Run("tuniq-workers1", func(b *testing.B) {
		args := []string{"-n", "200", "--workers", "1", inputFile}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if code := run(args, strings.NewReader(""), io.Discard, io.Discard); code != 0 {
				b.Fatalf("tuniq run failed with exit code %d", code)
			}
		}
	})

	b.Run("tuniq-workers8", func(b *testing.B) {
		args := []string{"-n", "200", "--workers", "8", inputFile}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if code := run(args, strings.NewReader(""), io.Discard, io.Discard); code != 0 {
				b.Fatalf("tuniq run failed with exit code %d", code)
			}
		}
	})

	b.Run("sort-uniq-sort", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := runUnixBaselinePipeline(inputFile); err != nil {
				b.Fatalf("unix pipeline run failed: %v", err)
			}
		}
	})
}

func runUnixBaselinePipeline(inputFile string) error {
	// Equivalent to: sort <file> | uniq -c | sort -rn > /dev/null
	cmd := exec.Command(
		"sh",
		"-c",
		fmt.Sprintf("LC_ALL=C sort %s | uniq -c | sort -rn > /dev/null", shellQuote(inputFile)),
	)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run()
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}
