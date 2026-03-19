package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := defaultConfig()

	if cfg.Workers != 8 {
		t.Errorf("Expected Workers to be 8, got %d", cfg.Workers)
	}
	if cfg.LogPath != "logs" {
		t.Errorf("Expected LogPath to be 'logs', got '%s'", cfg.LogPath)
	}
	if cfg.Key != "id" {
		t.Errorf("Expected Key to be 'id', got '%s'", cfg.Key)
	}
	if !cfg.CheckKey {
		t.Error("Expected CheckKey to be true")
	}
	if !cfg.CheckRow {
		t.Error("Expected CheckRow to be true")
	}
	if !cfg.ShowFolderBreakdown {
		t.Error("Expected ShowFolderBreakdown to be true")
	}
	if cfg.EnableTxtOutput {
		t.Error("Expected EnableTxtOutput to be false")
	}
	if cfg.EnableJsonOutput {
		t.Error("Expected EnableJsonOutput to be false")
	}
	if cfg.PurgeIDs {
		t.Error("Expected PurgeIDs to be false")
	}
	if cfg.PurgeRows {
		t.Error("Expected PurgeRows to be false")
	}
}

func TestLoad_FileNotExists(t *testing.T) {
	// Temporarily change working directory to avoid existing config
	tmpDir := t.TempDir()
	originalDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(originalDir)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Expected no error when config file doesn't exist, got %v", err)
	}

	// Should return default config
	if cfg.Workers != 8 {
		t.Errorf("Expected default Workers to be 8, got %d", cfg.Workers)
	}
}

func TestLoad_ValidConfig(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(originalDir)

	// Create config directory and file
	configDir := "config"
	os.MkdirAll(configDir, 0o755)

	testConfig := Config{
		Path:                "/test/path",
		Key:                 "test_key",
		Workers:             4,
		LogPath:             "test_logs",
		CheckKey:            false,
		CheckRow:            true,
		ShowFolderBreakdown: false,
		EnableTxtOutput:     true,
		EnableJsonOutput:    true,
		PurgeIDs:            true,
		PurgeRows:           false,
	}

	data, _ := json.MarshalIndent(testConfig, "", "  ")
	configPath := filepath.Join(configDir, "config.json")
	os.WriteFile(configPath, data, 0o644)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Expected no error loading config, got %v", err)
	}

	if cfg.Path != "/test/path" {
		t.Errorf("Expected Path to be '/test/path', got '%s'", cfg.Path)
	}
	if cfg.Key != "test_key" {
		t.Errorf("Expected Key to be 'test_key', got '%s'", cfg.Key)
	}
	if cfg.Workers != 4 {
		t.Errorf("Expected Workers to be 4, got %d", cfg.Workers)
	}
	if cfg.CheckKey {
		t.Error("Expected CheckKey to be false")
	}
	if cfg.EnableTxtOutput != true {
		t.Error("Expected EnableTxtOutput to be true")
	}
	if cfg.LoadedConfigPath == "" {
		t.Error("Expected LoadedConfigPath to be set for implicitly loaded config")
	}
	if !cfg.ConfigLoadedImplicitly {
		t.Error("Expected implicit config load to be recorded")
	}
}

func TestLoad_InvalidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(originalDir)

	// Create config directory and invalid JSON file
	configDir := "config"
	os.MkdirAll(configDir, 0o755)

	invalidJSON := `{"path": "/test", "workers": invalid_number}`
	configPath := filepath.Join(configDir, "config.json")
	os.WriteFile(configPath, []byte(invalidJSON), 0o644)

	_, err := Load()
	if err == nil {
		t.Fatal("Expected error for invalid JSON, got nil")
	}
}

func TestLoadWithOptions_ExplicitPath(t *testing.T) {
	tmpDir := t.TempDir()

	configPath := filepath.Join(tmpDir, "app.json")
	testConfig := Config{
		Key:     "id",
		LogPath: "explicit-logs",
		Workers: 3,
	}

	data, _ := json.MarshalIndent(testConfig, "", "  ")
	if err := os.WriteFile(configPath, data, 0o644); err != nil {
		t.Fatalf("failed to write explicit config: %v", err)
	}

	cfg, err := LoadWithOptions(LoadOptions{
		ExplicitPath:  configPath,
		AllowImplicit: false,
	})
	if err != nil {
		t.Fatalf("expected explicit load to succeed, got %v", err)
	}

	if got, want := cfg.LogPath, "explicit-logs"; got != want {
		t.Fatalf("expected log path %q, got %q", want, got)
	}
	if got, want := cfg.LoadedConfigPath, configPath; got != want {
		t.Fatalf("expected explicit loaded path %q, got %q", want, got)
	}
	if cfg.ConfigLoadedImplicitly {
		t.Fatal("expected explicit config load to not be marked implicit")
	}
}

func TestLoadWithOptions_DisallowImplicitSkipsConfigFile(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir, _ := os.Getwd()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir: %v", err)
	}
	defer os.Chdir(originalDir)

	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatalf("failed to create config dir: %v", err)
	}

	configPath := filepath.Join(configDir, configFile)
	testConfig := Config{
		LogPath: "implicit-logs",
	}
	data, _ := json.MarshalIndent(testConfig, "", "  ")
	if err := os.WriteFile(configPath, data, 0o644); err != nil {
		t.Fatalf("failed to write implicit config: %v", err)
	}

	cfg, err := LoadWithOptions(LoadOptions{AllowImplicit: false})
	if err != nil {
		t.Fatalf("expected no error when implicit loading is disabled, got %v", err)
	}

	if got, want := cfg.LogPath, "logs"; got != want {
		t.Fatalf("expected default log path %q, got %q", want, got)
	}
	if cfg.LoadedConfigPath != "" {
		t.Fatalf("expected no loaded config path, got %q", cfg.LoadedConfigPath)
	}
}

func TestLoadAdvanced_ValidConfig(t *testing.T) {
	tmpDir := t.TempDir()

	testConfig := Config{
		Path:    "/test/path",
		Workers: 4,
		Advanced: &AdvancedConfig{
			SearchTargets: []SearchTarget{
				{
					Name:          "test_ids",
					Type:          "direct",
					Path:          "$.id",
					TargetValues:  []string{"123", "456"},
					CaseSensitive: true,
				},
			},
			HashingStrategy: HashingStrategy{
				Mode:        "exclude_keys",
				ExcludeKeys: []string{"timestamp", "version"},
			},
			DeletionRules: []DeletionRule{
				{
					SearchTarget: "test_ids",
					Action:       "delete_row",
				},
			},
			SchemaDiscovery: SchemaDiscoveryConfig{
				Enabled:       true,
				SamplePercent: 0.05,
				MaxDepth:      5,
				MaxSamples:    50000,
				OutputFormats: []string{"json", "csv"},
			},
		},
	}

	data, _ := json.MarshalIndent(testConfig, "", "  ")
	configPath := filepath.Join(tmpDir, "advanced_config.json")
	os.WriteFile(configPath, data, 0o644)

	cfg, err := LoadAdvanced(configPath)
	if err != nil {
		t.Fatalf("Expected no error loading advanced config, got %v", err)
	}

	if cfg.Path != "/test/path" {
		t.Errorf("Expected Path to be '/test/path', got '%s'", cfg.Path)
	}
	if cfg.Workers != 4 {
		t.Errorf("Expected Workers to be 4, got %d", cfg.Workers)
	}
	if cfg.Advanced == nil {
		t.Fatal("Expected Advanced config to be loaded")
	}
	if len(cfg.Advanced.SearchTargets) != 1 {
		t.Errorf("Expected 1 search target, got %d", len(cfg.Advanced.SearchTargets))
	}
	if cfg.Advanced.SearchTargets[0].Name != "test_ids" {
		t.Errorf("Expected search target name 'test_ids', got '%s'", cfg.Advanced.SearchTargets[0].Name)
	}
	if cfg.Advanced.HashingStrategy.Mode != "exclude_keys" {
		t.Errorf("Expected hashing mode 'exclude_keys', got '%s'", cfg.Advanced.HashingStrategy.Mode)
	}
	if got, want := cfg.LoadedConfigPath, configPath; got != want {
		t.Errorf("Expected LoadedConfigPath %q, got %q", want, got)
	}
}

func TestLoadAdvanced_Defaults(t *testing.T) {
	tmpDir := t.TempDir()

	// Minimal config to test defaults
	testConfig := Config{
		Path: "/test/path",
		Advanced: &AdvancedConfig{
			SearchTargets: []SearchTarget{
				{
					Name:         "test",
					Type:         "direct",
					Path:         "$.id",
					TargetValues: []string{"123"},
				},
			},
		},
	}

	data, _ := json.MarshalIndent(testConfig, "", "  ")
	configPath := filepath.Join(tmpDir, "minimal_config.json")
	os.WriteFile(configPath, data, 0o644)

	cfg, err := LoadAdvanced(configPath)
	if err != nil {
		t.Fatalf("Expected no error loading config, got %v", err)
	}

	// Test base config defaults
	if cfg.Workers != 8 {
		t.Errorf("Expected default Workers to be 8, got %d", cfg.Workers)
	}
	if cfg.LogPath != "logs" {
		t.Errorf("Expected default LogPath to be 'logs', got '%s'", cfg.LogPath)
	}
	if cfg.Key != "id" {
		t.Errorf("Expected default Key to be 'id', got '%s'", cfg.Key)
	}

	// Test advanced config defaults
	if cfg.Advanced.HashingStrategy.Mode != "full_row" {
		t.Errorf("Expected default hashing mode 'full_row', got '%s'", cfg.Advanced.HashingStrategy.Mode)
	}
	if cfg.Advanced.SchemaDiscovery.SamplePercent != 0.1 {
		t.Errorf("Expected default sample percent 0.1, got %f", cfg.Advanced.SchemaDiscovery.SamplePercent)
	}
	if cfg.Advanced.SchemaDiscovery.MaxDepth != 10 {
		t.Errorf("Expected default max depth 10, got %d", cfg.Advanced.SchemaDiscovery.MaxDepth)
	}
	if cfg.Advanced.SchemaDiscovery.MaxSamples != 100000 {
		t.Errorf("Expected default max samples 100000, got %d", cfg.Advanced.SchemaDiscovery.MaxSamples)
	}
	if len(cfg.Advanced.SchemaDiscovery.OutputFormats) != 1 || cfg.Advanced.SchemaDiscovery.OutputFormats[0] != "json" {
		t.Errorf("Expected default output formats ['json'], got %v", cfg.Advanced.SchemaDiscovery.OutputFormats)
	}
}

func TestSave(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir, _ := os.Getwd()
	os.Chdir(tmpDir)
	defer os.Chdir(originalDir)

	cfg := &Config{
		Path:                "/test/path",
		Key:                 "test_key",
		Workers:             4,
		LogPath:             "test_logs",
		CheckKey:            false,
		CheckRow:            true,
		ShowFolderBreakdown: false,
		EnableTxtOutput:     true,
		EnableJsonOutput:    true,
		PurgeIDs:            true,
		PurgeRows:           false,
	}

	err := cfg.Save()
	if err != nil {
		t.Fatalf("Expected no error saving config, got %v", err)
	}

	// Check if config directory was created
	if _, err := os.Stat("config"); os.IsNotExist(err) {
		t.Error("Expected config directory to be created")
	}

	// Check if config file exists
	configPath := filepath.Join("config", "config.json")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Error("Expected config file to be created")
	}

	// Load and verify the saved config
	savedCfg, err := Load()
	if err != nil {
		t.Fatalf("Expected no error loading saved config, got %v", err)
	}

	if savedCfg.Path != "/test/path" {
		t.Errorf("Expected saved Path to be '/test/path', got '%s'", savedCfg.Path)
	}
	if savedCfg.Workers != 4 {
		t.Errorf("Expected saved Workers to be 4, got %d", savedCfg.Workers)
	}
}

func TestAdvancedConfig_Validate(t *testing.T) {
	tests := []struct {
		name      string
		config    *AdvancedConfig
		wantError bool
		errorMsg  string
	}{
		{
			name: "valid config",
			config: &AdvancedConfig{
				SearchTargets: []SearchTarget{
					{
						Name:         "test",
						Type:         "direct",
						Path:         "$.id",
						TargetValues: []string{"123"},
					},
				},
				HashingStrategy: HashingStrategy{
					Mode: "full_row",
				},
				DeletionRules: []DeletionRule{
					{
						SearchTarget: "test",
						Action:       "delete_row",
					},
				},
			},
			wantError: false,
		},
		{
			name: "empty search target name",
			config: &AdvancedConfig{
				SearchTargets: []SearchTarget{
					{
						Name:         "",
						Type:         "direct",
						Path:         "$.id",
						TargetValues: []string{"123"},
					},
				},
			},
			wantError: true,
			errorMsg:  "search target name cannot be empty",
		},
		{
			name: "duplicate search target names",
			config: &AdvancedConfig{
				SearchTargets: []SearchTarget{
					{
						Name:         "test",
						Type:         "direct",
						Path:         "$.id",
						TargetValues: []string{"123"},
					},
					{
						Name:         "test",
						Type:         "direct",
						Path:         "$.name",
						TargetValues: []string{"456"},
					},
				},
			},
			wantError: true,
			errorMsg:  "duplicate search target name: test",
		},
		{
			name: "empty search target type",
			config: &AdvancedConfig{
				SearchTargets: []SearchTarget{
					{
						Name:         "test",
						Type:         "",
						Path:         "$.id",
						TargetValues: []string{"123"},
					},
				},
			},
			wantError: true,
			errorMsg:  "search target type cannot be empty for target: test",
		},
		{
			name: "empty search target path",
			config: &AdvancedConfig{
				SearchTargets: []SearchTarget{
					{
						Name:         "test",
						Type:         "direct",
						Path:         "",
						TargetValues: []string{"123"},
					},
				},
			},
			wantError: true,
			errorMsg:  "search target path cannot be empty for target: test",
		},
		{
			name: "deletion rule references unknown target",
			config: &AdvancedConfig{
				SearchTargets: []SearchTarget{
					{
						Name:         "test",
						Type:         "direct",
						Path:         "$.id",
						TargetValues: []string{"123"},
					},
				},
				DeletionRules: []DeletionRule{
					{
						SearchTarget: "unknown",
						Action:       "delete_row",
					},
				},
			},
			wantError: true,
			errorMsg:  "deletion rule references unknown search target: unknown",
		},
		{
			name: "invalid hashing strategy mode",
			config: &AdvancedConfig{
				SearchTargets: []SearchTarget{
					{
						Name:         "test",
						Type:         "direct",
						Path:         "$.id",
						TargetValues: []string{"123"},
					},
				},
				HashingStrategy: HashingStrategy{
					Mode: "invalid_mode",
				},
			},
			wantError: true,
			errorMsg:  "invalid hashing strategy mode: invalid_mode",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				} else if err.Error() != tt.errorMsg {
					t.Errorf("Expected error message '%s', got '%s'", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
			}
		})
	}
}

func TestResolveApprovedOutputRootDefaultsToWorkingDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	originalDir, _ := os.Getwd()
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir: %v", err)
	}
	defer os.Chdir(originalDir)

	root, err := ResolveApprovedOutputRoot("")
	if err != nil {
		t.Fatalf("ResolveApprovedOutputRoot returned error: %v", err)
	}
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %v", err)
	}
	if got, want := root, cwd; got != want {
		t.Fatalf("expected approved root %q, got %q", want, got)
	}
}

func TestValidateLocalWriteTargetsRejectsEscapes(t *testing.T) {
	root := t.TempDir()
	outsideTarget := filepath.Join(root, "..", "escape.txt")

	_, err := ValidateLocalWriteTargets(root, []string{outsideTarget})
	if err == nil {
		t.Fatal("expected escaping local target to be rejected")
	}
}

func TestValidateLocalWriteTargetsReturnsAbsolutePaths(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "nested", "artifact.json")

	targets, err := ValidateLocalWriteTargets(root, []string{target, target})
	if err != nil {
		t.Fatalf("ValidateLocalWriteTargets returned error: %v", err)
	}
	if len(targets) != 1 {
		t.Fatalf("expected deduped targets, got %v", targets)
	}
	if got, want := targets[0], filepath.Clean(target); got != want {
		t.Fatalf("expected absolute target %q, got %q", want, got)
	}
}

func TestAdvancedConfig_setDefaults(t *testing.T) {
	cfg := &AdvancedConfig{}
	cfg.setDefaults()

	if cfg.HashingStrategy.Mode != "full_row" {
		t.Errorf("Expected default hashing mode 'full_row', got '%s'", cfg.HashingStrategy.Mode)
	}
	if cfg.SchemaDiscovery.SamplePercent != 0.1 {
		t.Errorf("Expected default sample percent 0.1, got %f", cfg.SchemaDiscovery.SamplePercent)
	}
	if cfg.SchemaDiscovery.MaxDepth != 10 {
		t.Errorf("Expected default max depth 10, got %d", cfg.SchemaDiscovery.MaxDepth)
	}
	if cfg.SchemaDiscovery.MaxSamples != 100000 {
		t.Errorf("Expected default max samples 100000, got %d", cfg.SchemaDiscovery.MaxSamples)
	}
	if len(cfg.SchemaDiscovery.OutputFormats) != 1 || cfg.SchemaDiscovery.OutputFormats[0] != "json" {
		t.Errorf("Expected default output formats ['json'], got %v", cfg.SchemaDiscovery.OutputFormats)
	}
}
