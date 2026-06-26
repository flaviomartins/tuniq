package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/flaviomartins/tuniq/pkg/output"
)

type Defaults struct {
	TopN             int
	ShowAll          bool
	Reverse          bool
	ShowCount        bool
	UpdateEvery      int
	Stats            bool
	StatsRSS         bool
	Progress         bool
	Workers          int
	MemoryLimitBytes uint64
	ProgressEvery    uint64
	ProgressSeconds  float64
	OutputMode       output.Mode
}

func DefaultSettings() Defaults {
	return Defaults{
		TopN:            -1,
		ShowCount:       true,
		Workers:         runtime.GOMAXPROCS(0),
		ProgressEvery:   1_000_000,
		OutputMode:      output.ModePlain,
		ProgressSeconds: 0,
	}
}

func LoadDefault() (Defaults, error) {
	settings := DefaultSettings()
	paths, err := configSearchPaths()
	if err != nil {
		return Defaults{}, err
	}
	for _, path := range paths {
		if err := parseInto(path, &settings); err != nil {
			return Defaults{}, err
		}
	}
	return settings, nil
}

func configSearchPaths() ([]string, error) {
	paths := make([]string, 0, 3)
	seen := make(map[string]struct{}, 3)
	addPath := func(path string) {
		if path == "" {
			return
		}
		if _, ok := seen[path]; ok {
			return
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}

	homeDir, err := os.UserHomeDir()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("resolve user home directory: %w", err)
	}
	if homeDir != "" {
		addPath(filepath.Join(homeDir, ".tuniqrc"))
	}

	userConfigDir, err := os.UserConfigDir()
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("resolve user config directory: %w", err)
	}
	if userConfigDir != "" {
		addPath(filepath.Join(userConfigDir, "tuniq", ".tuniq"))
	}

	// Keep current directory last so project-level settings override user defaults.
	addPath(".tuniqrc")
	return paths, nil
}

func parseInto(path string, settings *Defaults) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		_ = f.Close()
	}()

	scanner := bufio.NewScanner(f)
	for lineNum := 1; scanner.Scan(); lineNum++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, rawValue, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("%s:%d: expected key=value", path, lineNum)
		}
		key = strings.TrimSpace(key)
		value := strings.TrimSpace(rawValue)
		if err := assignSetting(settings, key, value); err != nil {
			return fmt.Errorf("%s:%d: %w", path, lineNum, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan %s: %w", path, err)
	}
	return nil
}

func assignSetting(settings *Defaults, key, value string) error {
	switch key {
	case "top_n":
		v, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid integer for top_n: %w", err)
		}
		settings.TopN = v
	case "show_all":
		v, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("invalid bool for show_all: %w", err)
		}
		settings.ShowAll = v
	case "reverse":
		v, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("invalid bool for reverse: %w", err)
		}
		settings.Reverse = v
	case "show_count":
		v, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("invalid bool for show_count: %w", err)
		}
		settings.ShowCount = v
	case "update_every":
		v, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid integer for update_every: %w", err)
		}
		settings.UpdateEvery = v
	case "stats":
		v, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("invalid bool for stats: %w", err)
		}
		settings.Stats = v
	case "stats_rss":
		v, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("invalid bool for stats_rss: %w", err)
		}
		settings.StatsRSS = v
	case "progress":
		v, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("invalid bool for progress: %w", err)
		}
		settings.Progress = v
	case "workers":
		v, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("invalid integer for workers: %w", err)
		}
		settings.Workers = v
	case "memory_limit_bytes":
		v, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid uint for memory_limit_bytes: %w", err)
		}
		settings.MemoryLimitBytes = v
	case "progress_every":
		v, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid uint for progress_every: %w", err)
		}
		settings.ProgressEvery = v
	case "progress_every_seconds":
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("invalid float for progress_every_seconds: %w", err)
		}
		settings.ProgressSeconds = v
	case "output":
		switch strings.ToLower(value) {
		case string(output.ModePlain):
			settings.OutputMode = output.ModePlain
		case string(output.ModeCSV):
			settings.OutputMode = output.ModeCSV
		case string(output.ModeJSON):
			settings.OutputMode = output.ModeJSON
		default:
			return fmt.Errorf("invalid output mode %q", value)
		}
	default:
		return fmt.Errorf("unknown key %q", key)
	}
	return nil
}
