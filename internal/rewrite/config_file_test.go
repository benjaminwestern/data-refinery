package rewrite

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigFileResolvesRelativePaths(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "configs")
	dataDir := filepath.Join(root, "data")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatalf("failed to create config dir: %v", err)
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("failed to create data dir: %v", err)
	}

	configPath := filepath.Join(configDir, "rewrite.json")
	configJSON := `{
  "paths": ["../data/orders.ndjson", "gs://example-bucket/orders"],
  "logPath": "../logs",
  "approvedOutputRoot": "../guarded-output",
  "backupDir": "../backups",
  "mode": "apply",
  "topLevelKey": "status",
  "topLevelValues": ["deleted"]
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfigFile(configPath)
	if err != nil {
		t.Fatalf("LoadConfigFile returned error: %v", err)
	}

	if got, want := cfg.Paths[0], filepath.Join(root, "data", "orders.ndjson"); got != want {
		t.Fatalf("expected first path %q, got %q", want, got)
	}
	if got := cfg.Paths[1]; got != "gs://example-bucket/orders" {
		t.Fatalf("expected GCS path to stay untouched, got %q", got)
	}
	if got, want := cfg.LogPath, filepath.Join(root, "logs"); got != want {
		t.Fatalf("expected log path %q, got %q", want, got)
	}
	if got, want := cfg.ApprovedOutputRoot, filepath.Join(root, "guarded-output"); got != want {
		t.Fatalf("expected approved output root %q, got %q", want, got)
	}
	if got, want := cfg.BackupDir, filepath.Join(root, "backups"); got != want {
		t.Fatalf("expected backup dir %q, got %q", want, got)
	}
}

func TestMergeConfigOverlaysPortableFileValues(t *testing.T) {
	base := &Config{
		Paths:   []string{"original"},
		Workers: 8,
		LogPath: "logs",
		Mode:    ModePreview,
	}
	override := &Config{
		Paths:              []string{"updated"},
		Workers:            2,
		LogPath:            "portable-logs",
		ApprovedOutputRoot: "portable-root",
		Mode:               ModeApply,
		BackupDir:          "backups",
		TopLevelKey:        "status",
		TopLevelValues:     []string{"deleted"},
	}

	MergeConfig(base, override)

	if got := base.Paths[0]; got != "updated" {
		t.Fatalf("expected merged paths to be updated, got %q", got)
	}
	if base.Workers != 2 || base.LogPath != "portable-logs" || base.Mode != ModeApply {
		t.Fatalf("unexpected merged config: %+v", base)
	}
	if got, want := base.ApprovedOutputRoot, "portable-root"; got != want {
		t.Fatalf("expected approved output root %q, got %q", want, got)
	}
	if base.TopLevelKey != "status" || len(base.TopLevelValues) != 1 || base.TopLevelValues[0] != "deleted" {
		t.Fatalf("expected delete settings to be merged, got %+v", base)
	}
}
