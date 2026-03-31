// Package config defines application configuration loading, validation, and defaults.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	configDir  = "config"
	configFile = "config.json"
)

// Config holds the user-configurable settings for an analysis run.
type Config struct {
	Path                        string `json:"path"`
	Key                         string `json:"key"`
	Workers                     int    `json:"workers"`
	LogPath                     string `json:"logPath"`
	ApprovedOutputRoot          string `json:"approvedOutputRoot,omitempty"`
	CheckKey                    bool   `json:"checkKey"`
	CheckRow                    bool   `json:"checkRow"`
	ValidateOnly                bool   `json:"validateOnly"`
	IsValidationRun             bool   `json:"isValidationRun"`
	ShowFolderBreakdown         bool   `json:"showFolderBreakdown"`
	EnableTxtOutput             bool   `json:"enableTxtOutput"`
	EnableJSONOutput            bool   `json:"enableJsonOutput"`
	PurgeIDs                    bool   `json:"purgeIds"`
	PurgeRows                   bool   `json:"purgeRows"`
	GCSAvailable                bool   `json:"-"` // Runtime flag; not persisted to config.
	LoadedConfigPath            string `json:"-"`
	ConfigLoadedImplicitly      bool   `json:"-"`
	AllowImplicitMutationConfig bool   `json:"-"`
	UnsafeMutationBypass        bool   `json:"-"`

	// Advanced analysis features.
	Advanced *AdvancedConfig `json:"advanced,omitempty"`

	// Performance configuration.
	Performance *PerformanceConfig `json:"performance,omitempty"`

	// Error handling configuration.
	ErrorHandling *ErrorHandlingConfig `json:"errorHandling,omitempty"`

	// State management configuration.
	StateManagement *StateManagementConfig `json:"stateManagement,omitempty"`

	// Memory management configuration.
	MemoryManagement *MemoryManagementConfig `json:"memoryManagement,omitempty"`
}

// LoadOptions controls how application configuration is discovered.
type LoadOptions struct {
	ExplicitPath  string
	AllowImplicit bool
}

// AdvancedConfig groups the optional analysis features beyond the base run.
type AdvancedConfig struct {
	// Search and deletion features.
	SearchTargets   []SearchTarget        `json:"searchTargets"`
	HashingStrategy HashingStrategy       `json:"hashingStrategy"`
	DeletionRules   []DeletionRule        `json:"deletionRules"`
	SchemaDiscovery SchemaDiscoveryConfig `json:"schemaDiscovery"`

	// Backup configuration.
	BackupConfig BackupConfig `json:"backup"`

	// Output configuration.
	OutputConfig OutputConfig `json:"output"`
}

// PerformanceConfig contains throughput and buffering settings.
type PerformanceConfig struct {
	// Worker pool configuration.
	MinWorkers     int           `json:"minWorkers"`
	MaxWorkers     int           `json:"maxWorkers"`
	WorkerIdleTime time.Duration `json:"workerIdleTime"`
	TaskBufferSize int           `json:"taskBufferSize"`

	// Memory configuration.
	MaxMemoryUsage  int64   `json:"maxMemoryUsage"`  // Bytes.
	MemoryThreshold float64 `json:"memoryThreshold"` // Fraction between 0 and 1.

	// File processing configuration.
	BufferSize    int `json:"bufferSize"`
	MaxBufferSize int `json:"maxBufferSize"`
	BatchSize     int `json:"batchSize"`

	// Adaptive scaling.
	EnableAdaptiveScaling bool          `json:"enableAdaptiveScaling"`
	ScalingFactor         float64       `json:"scalingFactor"`
	MetricsInterval       time.Duration `json:"metricsInterval"`
}

// ErrorHandlingConfig contains retry, tolerance, and alerting settings.
type ErrorHandlingConfig struct {
	// Retry configuration.
	MaxRetries   int           `json:"maxRetries"`
	RetryDelay   time.Duration `json:"retryDelay"`
	RetryBackoff float64       `json:"retryBackoff"`

	// Circuit breaker configuration.
	CircuitBreakerThreshold int           `json:"circuitBreakerThreshold"`
	CircuitBreakerTimeout   time.Duration `json:"circuitBreakerTimeout"`

	// Error tolerance.
	MaxErrorsPerFile int     `json:"maxErrorsPerFile"`
	MaxErrorRate     float64 `json:"maxErrorRate"`
	ContinueOnError  bool    `json:"continueOnError"`

	// Alerting configuration.
	AlertingConfig AlertingConfig `json:"alerting"`
}

// StateManagementConfig contains checkpointing and persisted run-state settings.
type StateManagementConfig struct {
	Enabled             bool          `json:"enabled"`
	StateDir            string        `json:"stateDir"`
	AutoSaveInterval    time.Duration `json:"autoSaveInterval"`
	MaxStateHistory     int           `json:"maxStateHistory"`
	CompressionLevel    int           `json:"compressionLevel"`
	EnableCheckpointing bool          `json:"enableCheckpointing"`
	CheckpointInterval  time.Duration `json:"checkpointInterval"`
}

// MemoryManagementConfig contains streaming and memory-pressure settings.
type MemoryManagementConfig struct {
	EnableStreaming         bool          `json:"enableStreaming"`
	StreamingThreshold      int64         `json:"streamingThreshold"`
	MaxMemoryUsage          int64         `json:"maxMemoryUsage"`
	MemoryPressureThreshold float64       `json:"memoryPressureThreshold"`
	GCInterval              time.Duration `json:"gcInterval"`
	EnableMemoryProfiling   bool          `json:"enableMemoryProfiling"`
}

// SearchTarget defines what to search for and where to search for it.
type SearchTarget struct {
	Name          string   `json:"name"`
	Type          string   `json:"type"` // "direct", "nested_array", "nested_object", or "jsonpath".
	Path          string   `json:"path"` // JSONPath-like syntax.
	TargetValues  []string `json:"targetValues"`
	CaseSensitive bool     `json:"caseSensitive"`
}

// HashingStrategy defines how rows are normalised and hashed for deduplication.
type HashingStrategy struct {
	Mode        string   `json:"mode"` // "full_row", "selective", or "exclude_keys".
	IncludeKeys []string `json:"includeKeys,omitempty"`
	ExcludeKeys []string `json:"excludeKeys,omitempty"`
	Algorithm   string   `json:"algorithm"` // "fnv", "md5", or "sha256".
	Normalize   bool     `json:"normalize"` // Normalise data before hashing.
}

// DeletionRule defines the derived output to produce for matching records.
type DeletionRule struct {
	SearchTarget string   `json:"searchTarget"` // References SearchTarget.Name.
	Action       string   `json:"action"`       // "delete_row", "delete_matches", "mark_for_deletion", or "delete_sub_key".
	OutputPath   string   `json:"outputPath,omitempty"`
	SubKeyPath   string   `json:"subKeyPath,omitempty"`   // Used for sub-key targeted deletions.
	SubKeyValues []string `json:"subKeyValues,omitempty"` // Values to match for sub-key deletion.
}

// SchemaDiscoveryConfig controls schema sampling and export behaviour.
type SchemaDiscoveryConfig struct {
	Enabled       bool     `json:"enabled"`
	SamplePercent float64  `json:"samplePercent"` // 0.1 = 10%.
	MaxDepth      int      `json:"maxDepth"`      // Limit nesting depth.
	MaxSamples    int      `json:"maxSamples"`    // Cap total samples.
	OutputFormats []string `json:"outputFormats"` // For example: ["json", "csv", "yaml"].
	GroupByFolder bool     `json:"groupByFolder"` // Emit per-folder schema views.
}

// BackupConfig contains analysis backup settings.
type BackupConfig struct {
	Enabled       bool   `json:"enabled"`
	BackupDir     string `json:"backupDir"`
	RetentionDays int    `json:"retentionDays"`
	Compression   bool   `json:"compression"`
}

// OutputConfig contains output destination settings.
type OutputConfig struct {
	Formats     []string `json:"formats"`
	Destination string   `json:"destination"`
	Compression bool     `json:"compression"`
}

// AlertingConfig contains alerting rules and notification routes.
type AlertingConfig struct {
	Enabled     bool                  `json:"enabled"`
	Rules       []AlertRule           `json:"rules"`
	Channels    []NotificationChannel `json:"channels"`
	Escalations []EscalationPolicy    `json:"escalations"`
}

// AlertRule represents a single alerting rule.
type AlertRule struct {
	Name      string            `json:"name"`
	Condition string            `json:"condition"`
	Threshold float64           `json:"threshold"`
	Severity  string            `json:"severity"`
	Enabled   bool              `json:"enabled"`
	Labels    map[string]string `json:"labels"`
}

// NotificationChannel represents a notification channel.
type NotificationChannel struct {
	Name     string         `json:"name"`
	Type     string         `json:"type"`
	Settings map[string]any `json:"settings"`
	Enabled  bool           `json:"enabled"`
}

// EscalationPolicy represents an escalation policy.
type EscalationPolicy struct {
	Name     string        `json:"name"`
	Rules    []string      `json:"rules"`
	Channels []string      `json:"channels"`
	Delay    time.Duration `json:"delay"`
}

// Load reads the application configuration from the supported config locations
// and falls back to built-in defaults when none are present.
func Load() (*Config, error) {
	return LoadWithOptions(LoadOptions{AllowImplicit: true})
}

// LoadWithOptions reads the application configuration using the requested trust
// policy and falls back to built-in defaults when no file is loaded.
func LoadWithOptions(options LoadOptions) (*Config, error) {
	cfg := defaultConfig()

	// Load from environment variables
	if err := loadFromEnv(cfg); err != nil {
		return nil, fmt.Errorf("failed to load from environment: %w", err)
	}

	explicitPath := strings.TrimSpace(options.ExplicitPath)
	if explicitPath != "" {
		loadedPath, err := loadFileAtPath(cfg, explicitPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load explicit config file: %w", err)
		}
		cfg.LoadedConfigPath = loadedPath
		cfg.ConfigLoadedImplicitly = false
	} else if options.AllowImplicit {
		loadedPath, err := loadFromFile(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to load from file: %w", err)
		}
		if loadedPath != "" {
			cfg.LoadedConfigPath = loadedPath
			cfg.ConfigLoadedImplicitly = true
		}
	}

	// Set defaults for advanced config if present
	if cfg.Advanced != nil {
		cfg.Advanced.setDefaults()
	}

	// Set defaults for performance config if not provided
	if cfg.Performance == nil {
		cfg.Performance = &PerformanceConfig{
			MinWorkers:            1,
			MaxWorkers:            cfg.Workers * 2,
			WorkerIdleTime:        30 * time.Second,
			TaskBufferSize:        1000,
			MaxMemoryUsage:        2 * 1024 * 1024 * 1024, // 2GB
			MemoryThreshold:       0.8,
			BufferSize:            1024 * 1024,      // 1MB
			MaxBufferSize:         10 * 1024 * 1024, // 10MB
			BatchSize:             100,
			EnableAdaptiveScaling: true,
			ScalingFactor:         1.5,
			MetricsInterval:       10 * time.Second,
		}
	}

	// Set defaults for error handling config if not provided
	if cfg.ErrorHandling == nil {
		cfg.ErrorHandling = &ErrorHandlingConfig{
			MaxRetries:              3,
			RetryDelay:              time.Second,
			RetryBackoff:            2.0,
			CircuitBreakerThreshold: 5,
			CircuitBreakerTimeout:   30 * time.Second,
			MaxErrorsPerFile:        100,
			MaxErrorRate:            0.1,
			ContinueOnError:         true,
			AlertingConfig: AlertingConfig{
				Enabled: false,
			},
		}
	}

	// Set defaults for state management config if not provided
	if cfg.StateManagement == nil {
		cfg.StateManagement = &StateManagementConfig{
			Enabled:             true,
			StateDir:            "state",
			AutoSaveInterval:    30 * time.Second,
			MaxStateHistory:     10,
			CompressionLevel:    6,
			EnableCheckpointing: true,
			CheckpointInterval:  5 * time.Minute,
		}
	}

	// Set defaults for memory management config if not provided
	if cfg.MemoryManagement == nil {
		cfg.MemoryManagement = &MemoryManagementConfig{
			EnableStreaming:         true,
			StreamingThreshold:      100 * 1024 * 1024,      // 100MB
			MaxMemoryUsage:          2 * 1024 * 1024 * 1024, // 2GB
			MemoryPressureThreshold: 0.8,
			GCInterval:              5 * time.Minute,
			EnableMemoryProfiling:   false,
		}
	}

	// Validate configuration
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return cfg, nil
}

// loadFromEnv loads configuration from environment variables.
func loadFromEnv(config *Config) error {
	if path := os.Getenv("DATA_REFINERY_PATH"); path != "" {
		config.Path = path
	}
	if key := os.Getenv("DATA_REFINERY_KEY"); key != "" {
		config.Key = key
	}
	if workers := os.Getenv("DATA_REFINERY_WORKERS"); workers != "" {
		w, err := strconv.Atoi(workers)
		if err != nil {
			return fmt.Errorf("invalid DATA_REFINERY_WORKERS value %q: %w", workers, err)
		}
		config.Workers = w
	}
	if logPath := os.Getenv("DATA_REFINERY_LOG_PATH"); logPath != "" {
		config.LogPath = logPath
	}
	if checkKey := os.Getenv("DATA_REFINERY_CHECK_KEY"); checkKey != "" {
		config.CheckKey = strings.ToLower(checkKey) == "true"
	}
	if checkRow := os.Getenv("DATA_REFINERY_CHECK_ROW"); checkRow != "" {
		config.CheckRow = strings.ToLower(checkRow) == "true"
	}

	return nil
}

// loadFromFile loads configuration from the first available JSON config file.
func loadFromFile(config *Config) (string, error) {
	configPaths := []string{
		filepath.Join(configDir, configFile),
		"config.json",
		"data-refinery.json",
		filepath.Join(os.Getenv("HOME"), ".data-refinery.json"),
		"/etc/data-refinery/config.json",
	}

	for _, path := range configPaths {
		//nolint:gosec // Config files are intentionally loaded from explicit user/system paths.
		if _, err := os.Stat(path); err == nil {
			return loadFileAtPath(config, path)
		}
	}

	return "", nil // No config file found; use defaults.
}

// validateConfig validates the configuration.
func validateConfig(config *Config) error {
	if config.Workers < 1 {
		return fmt.Errorf("workers must be at least 1")
	}
	if config.Workers > 100 {
		return fmt.Errorf("workers cannot exceed 100")
	}
	if config.Key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	if config.LogPath == "" {
		return fmt.Errorf("log path cannot be empty")
	}

	// Validate advanced configuration
	if config.Advanced != nil {
		if err := config.Advanced.Validate(); err != nil {
			return err
		}
	}

	// Validate performance configuration
	if config.Performance != nil {
		if config.Performance.MinWorkers < 1 {
			return fmt.Errorf("min workers must be at least 1")
		}
		if config.Performance.MaxWorkers < config.Performance.MinWorkers {
			return fmt.Errorf("max workers must be at least min workers")
		}
		if config.Performance.MemoryThreshold < 0 || config.Performance.MemoryThreshold > 1 {
			return fmt.Errorf("memory threshold must be between 0 and 1")
		}
	}

	// Validate error handling configuration
	if config.ErrorHandling != nil {
		if config.ErrorHandling.MaxRetries < 0 {
			return fmt.Errorf("max retries cannot be negative")
		}
		if config.ErrorHandling.RetryBackoff < 1 {
			return fmt.Errorf("retry backoff must be at least 1")
		}
		if config.ErrorHandling.MaxErrorRate < 0 || config.ErrorHandling.MaxErrorRate > 1 {
			return fmt.Errorf("max error rate must be between 0 and 1")
		}
	}

	return nil
}

// LoadAdvanced loads a JSON config file intended for advanced analysis runs.
func LoadAdvanced(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Set defaults for base config
	if cfg.Workers == 0 {
		cfg.Workers = 8
	}
	if cfg.LogPath == "" {
		cfg.LogPath = "logs"
	}
	if cfg.Key == "" {
		cfg.Key = "id"
	}

	// Set defaults for advanced config if present
	if cfg.Advanced != nil {
		cfg.Advanced.setDefaults()
	}

	cfg.LoadedConfigPath = normalizeConfigPath(path)
	cfg.ConfigLoadedImplicitly = false

	return &cfg, nil
}

func loadFileAtPath(config *Config, path string) (string, error) {
	//nolint:gosec // Config discovery intentionally reads explicit local file paths.
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	if err := json.Unmarshal(data, config); err != nil {
		return "", fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	return normalizeConfigPath(path), nil
}

func normalizeConfigPath(path string) string {
	if absPath, err := filepath.Abs(path); err == nil {
		return absPath
	}
	return path
}

// Save writes the configuration to `config/config.json`.
func (c *Config) Save() error {
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	configPath := filepath.Join(configDir, configFile)
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config to json: %w", err)
	}

	if err := os.WriteFile(configPath, data, 0o600); err != nil {
		return fmt.Errorf("failed to write config file to %s: %w", configPath, err)
	}

	return nil
}

// defaultConfig returns a new Config struct with baseline default values.
func defaultConfig() *Config {
	return &Config{
		Workers:             8,
		LogPath:             "logs",
		CheckKey:            true,
		CheckRow:            true,
		ShowFolderBreakdown: true,
		EnableTxtOutput:     false,
		EnableJSONOutput:    false,
		PurgeIDs:            false,
		PurgeRows:           false,
		Key:                 "id",
	}
}

// OutputFormat represents an output format and its target filename.
type OutputFormat struct {
	Format   string `json:"format"`
	Filename string `json:"filename"`
}

// setDefaults applies defaults to an advanced analysis config.
func (c *AdvancedConfig) setDefaults() {
	if c.HashingStrategy.Mode == "" {
		c.HashingStrategy.Mode = "full_row"
	}
	if c.HashingStrategy.Algorithm == "" {
		c.HashingStrategy.Algorithm = "fnv"
	}

	if c.SchemaDiscovery.SamplePercent == 0 {
		c.SchemaDiscovery.SamplePercent = 0.1 // 10%
	}

	if c.SchemaDiscovery.MaxDepth == 0 {
		c.SchemaDiscovery.MaxDepth = 10
	}

	if c.SchemaDiscovery.MaxSamples == 0 {
		c.SchemaDiscovery.MaxSamples = 100000
	}

	if len(c.SchemaDiscovery.OutputFormats) == 0 {
		c.SchemaDiscovery.OutputFormats = []string{"json"}
	}

	if c.BackupConfig.BackupDir == "" {
		c.BackupConfig.BackupDir = "backups"
	}
	if c.BackupConfig.RetentionDays == 0 {
		c.BackupConfig.RetentionDays = 30
	}

	if c.OutputConfig.Destination == "" {
		c.OutputConfig.Destination = "output"
	}
	if len(c.OutputConfig.Formats) == 0 {
		c.OutputConfig.Formats = []string{"txt"}
	}
}

// Validate checks whether the advanced analysis configuration is valid.
func (c *AdvancedConfig) Validate() error {
	// Validate search targets
	targetNames := make(map[string]bool)
	for _, target := range c.SearchTargets {
		if target.Name == "" {
			return fmt.Errorf("search target name cannot be empty")
		}
		if targetNames[target.Name] {
			return fmt.Errorf("duplicate search target name: %s", target.Name)
		}
		targetNames[target.Name] = true

		if target.Type == "" {
			return fmt.Errorf("search target type cannot be empty for target: %s", target.Name)
		}
		if target.Path == "" {
			return fmt.Errorf("search target path cannot be empty for target: %s", target.Name)
		}
	}

	// Validate deletion rules reference valid targets
	for _, rule := range c.DeletionRules {
		if !targetNames[rule.SearchTarget] {
			return fmt.Errorf("deletion rule references unknown search target: %s", rule.SearchTarget)
		}
	}

	// Validate hashing strategy
	switch c.HashingStrategy.Mode {
	case "full_row", "selective", "exclude_keys":
		// Valid modes
	default:
		return fmt.Errorf("invalid hashing strategy mode: %s", c.HashingStrategy.Mode)
	}

	if c.HashingStrategy.Mode == "selective" && len(c.HashingStrategy.IncludeKeys) == 0 {
		return fmt.Errorf("selective hashing requires include keys")
	}
	if c.HashingStrategy.Mode == "exclude_keys" && len(c.HashingStrategy.ExcludeKeys) == 0 {
		return fmt.Errorf("exclude_keys hashing requires exclude keys")
	}

	return nil
}

// IsAdvancedEnabled reports whether any advanced analysis feature is enabled.
func (c *Config) IsAdvancedEnabled() bool {
	return c.Advanced != nil && (c.Advanced.SchemaDiscovery.Enabled ||
		len(c.Advanced.SearchTargets) > 0 ||
		len(c.Advanced.DeletionRules) > 0 ||
		c.Advanced.BackupConfig.Enabled)
}

// GetEffectiveWorkers returns the worker count after performance limits apply.
func (c *Config) GetEffectiveWorkers() int {
	if c.Performance != nil {
		return minInt(maxInt(c.Workers, c.Performance.MinWorkers), c.Performance.MaxWorkers)
	}
	return c.Workers
}

// GetStateDir returns the directory used for persisted run state.
func (c *Config) GetStateDir() string {
	if c.StateManagement != nil && c.StateManagement.StateDir != "" {
		return c.StateManagement.StateDir
	}
	return filepath.Join(c.LogPath, "state")
}

// GetBackupDir returns the backup directory for derived analysis artefacts.
func (c *Config) GetBackupDir() string {
	if c.Advanced != nil && c.Advanced.BackupConfig.BackupDir != "" {
		return c.Advanced.BackupConfig.BackupDir
	}
	return filepath.Join(c.LogPath, "backups")
}

// Clone creates a deep copy of the configuration.
func (c *Config) Clone() *Config {
	data, err := json.Marshal(c)
	if err != nil {
		clone := *c
		return &clone
	}
	var clone Config
	if err := json.Unmarshal(data, &clone); err != nil {
		clone = *c
	}
	return &clone
}

// Merge overlays another configuration onto this one.
func (c *Config) Merge(other *Config) {
	if other.Path != "" {
		c.Path = other.Path
	}
	if other.Key != "" {
		c.Key = other.Key
	}
	if other.Workers != 0 {
		c.Workers = other.Workers
	}
	if other.LogPath != "" {
		c.LogPath = other.LogPath
	}

	// Merge boolean flags
	c.CheckKey = other.CheckKey
	c.CheckRow = other.CheckRow
	c.ShowFolderBreakdown = other.ShowFolderBreakdown
	c.EnableTxtOutput = other.EnableTxtOutput
	c.EnableJSONOutput = other.EnableJSONOutput
	c.PurgeIDs = other.PurgeIDs
	c.PurgeRows = other.PurgeRows

	// Merge advanced configuration
	if other.Advanced != nil {
		if c.Advanced == nil {
			c.Advanced = &AdvancedConfig{}
		}
		// Deep merge advanced config would go here
	}

	// Merge performance configuration
	if other.Performance != nil {
		if c.Performance == nil {
			c.Performance = &PerformanceConfig{}
		}
		// Deep merge performance config would go here
	}

	// Merge error handling configuration
	if other.ErrorHandling != nil {
		if c.ErrorHandling == nil {
			c.ErrorHandling = &ErrorHandlingConfig{}
		}
		// Deep merge error handling config would go here
	}

	// Merge state management configuration
	if other.StateManagement != nil {
		if c.StateManagement == nil {
			c.StateManagement = &StateManagementConfig{}
		}
		// Deep merge state management config would go here
	}

	// Merge memory management configuration
	if other.MemoryManagement != nil {
		if c.MemoryManagement == nil {
			c.MemoryManagement = &MemoryManagementConfig{}
		}
		// Deep merge memory management config would go here
	}
}

// Helper functions for min/max since they're not available in older Go versions.
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
