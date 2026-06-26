package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/flaviomartins/tuniq/pkg/output"
)

func isolateConfigEnv(t *testing.T, base string) {
	t.Helper()
	home := filepath.Join(base, "home")
	xdg := filepath.Join(base, "xdg")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatalf("mkdir home failed: %v", err)
	}
	if err := os.MkdirAll(xdg, 0o755); err != nil {
		t.Fatalf("mkdir xdg failed: %v", err)
	}
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("XDG_CONFIG_HOME", xdg)
	t.Setenv("APPDATA", xdg)
}

func TestLoadDefaultParsesDotTuniqrc(t *testing.T) {
	tempDir := t.TempDir()
	isolateConfigEnv(t, tempDir)
	origWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd failed: %v", err)
	}
	defer func() {
		_ = os.Chdir(origWD)
	}()
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("chdir failed: %v", err)
	}
	content := "" +
		"top_n=10\n" +
		"workers=3\n" +
		"progress=true\n" +
		"progress_every=200\n" +
		"output=csv\n"
	if err := os.WriteFile(filepath.Join(tempDir, ".tuniqrc"), []byte(content), 0o644); err != nil {
		t.Fatalf("write file failed: %v", err)
	}

	settings, err := LoadDefault()
	if err != nil {
		t.Fatalf("LoadDefault failed: %v", err)
	}
	if settings.TopN != 10 || settings.Workers != 3 || !settings.Progress || settings.ProgressEvery != 200 {
		t.Fatalf("unexpected parsed settings: %+v", settings)
	}
	if settings.OutputMode != output.ModeCSV {
		t.Fatalf("expected csv output defaults, got mode=%s", settings.OutputMode)
	}
}

func TestLoadDefaultRejectsInvalidLine(t *testing.T) {
	tempDir := t.TempDir()
	isolateConfigEnv(t, tempDir)
	origWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd failed: %v", err)
	}
	defer func() {
		_ = os.Chdir(origWD)
	}()
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("chdir failed: %v", err)
	}
	if err := os.WriteFile(".tuniqrc", []byte("invalid_line"), 0o644); err != nil {
		t.Fatalf("write file failed: %v", err)
	}

	if _, err := LoadDefault(); err == nil {
		t.Fatalf("expected parse error")
	}
}

func TestLoadDefaultSearchesHomeAndXDG(t *testing.T) {
	tempDir := t.TempDir()
	isolateConfigEnv(t, tempDir)
	home := filepath.Join(tempDir, "home")
	userConfigDir, err := os.UserConfigDir()
	if err != nil {
		t.Fatalf("UserConfigDir failed: %v", err)
	}
	configDir := filepath.Join(userConfigDir, "tuniq")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatalf("mkdir user config failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(home, ".tuniqrc"), []byte("top_n=5\n"), 0o644); err != nil {
		t.Fatalf("write home config failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(configDir, ".tuniq"), []byte("top_n=7\n"), 0o644); err != nil {
		t.Fatalf("write user config failed: %v", err)
	}

	settings, err := LoadDefault()
	if err != nil {
		t.Fatalf("LoadDefault failed: %v", err)
	}
	if settings.TopN != 7 {
		t.Fatalf("expected XDG config to override home config, got top_n=%d", settings.TopN)
	}
}
