package rewrite

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type fileConfig struct {
	Path               string   `json:"path"`
	Paths              []string `json:"paths"`
	Workers            int      `json:"workers"`
	LogPath            string   `json:"logPath"`
	ApprovedOutputRoot string   `json:"approvedOutputRoot"`
	XMLRecordPath      string   `json:"xmlRecordPath"`
	Mode               Mode     `json:"mode"`
	BackupDir          string   `json:"backupDir"`
	BufferSize         int      `json:"bufferSize"`
	MaxBufferSize      int      `json:"maxBufferSize"`
	TopLevelKey        string   `json:"topLevelKey"`
	TopLevelValues     []string `json:"topLevelValues"`
	ArrayKey           string   `json:"arrayKey"`
	ArrayDeleteKey     string   `json:"arrayDeleteKey"`
	ArrayDeleteValues  []string `json:"arrayDeleteValues"`
	StateKey           string   `json:"stateKey"`
	StateValue         string   `json:"stateValue"`
	UpdateKey          string   `json:"updateKey"`
	UpdateOldValue     string   `json:"updateOldValue"`
	UpdateNewValue     string   `json:"updateNewValue"`
	UpdateIDKey        string   `json:"updateIDKey"`
	UpdateIDValues     []string `json:"updateIDValues"`
	UpdateStateKey     string   `json:"updateStateKey"`
	UpdateStateValue   string   `json:"updateStateValue"`
}

// LoadConfigFile reads a rewrite config and resolves local relative paths
// against the config file location so the file stays portable across machines.
func LoadConfigFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read rewrite config: %w", err)
	}

	var raw fileConfig
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse rewrite config: %w", err)
	}

	configDir := filepath.Dir(path)
	paths := raw.Paths
	if len(paths) == 0 && raw.Path != "" {
		paths = parseCommaSeparatedValues(raw.Path)
	}

	cfg := &Config{
		Paths:              resolvePathsRelativeTo(configDir, paths),
		Workers:            raw.Workers,
		LogPath:            resolveLocalPath(configDir, raw.LogPath),
		ApprovedOutputRoot: resolveLocalPath(configDir, raw.ApprovedOutputRoot),
		XMLRecordPath:      raw.XMLRecordPath,
		Mode:               raw.Mode,
		BackupDir:          resolveLocalPath(configDir, raw.BackupDir),
		BufferSize:         raw.BufferSize,
		MaxBufferSize:      raw.MaxBufferSize,
		TopLevelKey:        raw.TopLevelKey,
		TopLevelValues:     cloneStrings(raw.TopLevelValues),
		ArrayKey:           raw.ArrayKey,
		ArrayDeleteKey:     raw.ArrayDeleteKey,
		ArrayDeleteValues:  cloneStrings(raw.ArrayDeleteValues),
		StateKey:           raw.StateKey,
		StateValue:         raw.StateValue,
		UpdateKey:          raw.UpdateKey,
		UpdateOldValue:     raw.UpdateOldValue,
		UpdateNewValue:     raw.UpdateNewValue,
		UpdateIDKey:        raw.UpdateIDKey,
		UpdateIDValues:     cloneStrings(raw.UpdateIDValues),
		UpdateStateKey:     raw.UpdateStateKey,
		UpdateStateValue:   raw.UpdateStateValue,
	}

	return cfg, nil
}

// MergeConfig overlays non-zero values from override onto base.
func MergeConfig(base, override *Config) {
	if override == nil {
		return
	}

	if override.Paths != nil {
		base.Paths = cloneStrings(override.Paths)
	}
	if override.Workers > 0 {
		base.Workers = override.Workers
	}
	if override.LogPath != "" {
		base.LogPath = override.LogPath
	}
	if override.ApprovedOutputRoot != "" {
		base.ApprovedOutputRoot = override.ApprovedOutputRoot
	}
	if override.XMLRecordPath != "" {
		base.XMLRecordPath = override.XMLRecordPath
	}
	if override.Mode != "" {
		base.Mode = override.Mode
	}
	if override.BackupDir != "" {
		base.BackupDir = override.BackupDir
	}
	if override.BufferSize > 0 {
		base.BufferSize = override.BufferSize
	}
	if override.MaxBufferSize > 0 {
		base.MaxBufferSize = override.MaxBufferSize
	}
	if override.TopLevelKey != "" {
		base.TopLevelKey = override.TopLevelKey
	}
	if override.TopLevelValues != nil {
		base.TopLevelValues = cloneStrings(override.TopLevelValues)
	}
	if override.ArrayKey != "" {
		base.ArrayKey = override.ArrayKey
	}
	if override.ArrayDeleteKey != "" {
		base.ArrayDeleteKey = override.ArrayDeleteKey
	}
	if override.ArrayDeleteValues != nil {
		base.ArrayDeleteValues = cloneStrings(override.ArrayDeleteValues)
	}
	if override.StateKey != "" {
		base.StateKey = override.StateKey
	}
	if override.StateValue != "" {
		base.StateValue = override.StateValue
	}
	if override.UpdateKey != "" {
		base.UpdateKey = override.UpdateKey
	}
	if override.UpdateOldValue != "" {
		base.UpdateOldValue = override.UpdateOldValue
	}
	if override.UpdateNewValue != "" {
		base.UpdateNewValue = override.UpdateNewValue
	}
	if override.UpdateIDKey != "" {
		base.UpdateIDKey = override.UpdateIDKey
	}
	if override.UpdateIDValues != nil {
		base.UpdateIDValues = cloneStrings(override.UpdateIDValues)
	}
	if override.UpdateStateKey != "" {
		base.UpdateStateKey = override.UpdateStateKey
	}
	if override.UpdateStateValue != "" {
		base.UpdateStateValue = override.UpdateStateValue
	}
}

func resolvePathsRelativeTo(baseDir string, paths []string) []string {
	if paths == nil {
		return nil
	}

	resolved := make([]string, 0, len(paths))
	for _, path := range paths {
		resolved = append(resolved, resolveLocalPath(baseDir, path))
	}
	return resolved
}

func resolveLocalPath(baseDir, path string) string {
	if path == "" || filepath.IsAbs(path) || isRemotePath(path) {
		return path
	}
	return filepath.Clean(filepath.Join(baseDir, path))
}

func isRemotePath(path string) bool {
	return strings.HasPrefix(path, "gs://")
}

func cloneStrings(values []string) []string {
	if values == nil {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}
