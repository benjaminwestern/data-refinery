package ingest

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
	OutputPath         string   `json:"outputPath"`
	MappingFile        string   `json:"mappingFile"`
	RequireMappings    bool     `json:"requireMappings"`
	StatsOutputPath    string   `json:"statsOutputPath"`
}

// LoadConfigFile reads an ingest config and resolves local relative paths
// against the config file location so the file stays portable across machines.
func LoadConfigFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ingest config: %w", err)
	}

	var raw fileConfig
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse ingest config: %w", err)
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
		OutputPath:         resolveAnyPath(configDir, raw.OutputPath),
		MappingFile:        resolveLocalPath(configDir, raw.MappingFile),
		RequireMappings:    raw.RequireMappings,
		StatsOutputPath:    resolveAnyPath(configDir, raw.StatsOutputPath),
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
	if override.OutputPath != "" {
		base.OutputPath = override.OutputPath
	}
	if override.MappingFile != "" {
		base.MappingFile = override.MappingFile
	}
	if override.RequireMappings {
		base.RequireMappings = true
	}
	if override.StatsOutputPath != "" {
		base.StatsOutputPath = override.StatsOutputPath
	}
}

func parseCommaSeparatedValues(input string) []string {
	if strings.TrimSpace(input) == "" {
		return nil
	}

	parts := strings.Split(input, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			values = append(values, trimmed)
		}
	}

	return values
}

func resolvePathsRelativeTo(baseDir string, paths []string) []string {
	if paths == nil {
		return nil
	}

	resolved := make([]string, 0, len(paths))
	for _, path := range paths {
		resolved = append(resolved, resolveAnyPath(baseDir, path))
	}
	return resolved
}

func resolveAnyPath(baseDir, path string) string {
	if path == "" || filepath.IsAbs(path) || isRemotePath(path) {
		return path
	}
	return filepath.Clean(filepath.Join(baseDir, path))
}

func resolveLocalPath(baseDir, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}
	return filepath.Clean(filepath.Join(baseDir, path))
}

func isRemotePath(path string) bool {
	return strings.HasPrefix(strings.TrimSpace(path), "gs://")
}

func cloneStrings(values []string) []string {
	if values == nil {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}
