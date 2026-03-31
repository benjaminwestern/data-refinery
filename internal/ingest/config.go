// Package ingest defines file-normalisation configuration and ingest execution.
package ingest

import (
	"fmt"
	"path/filepath"
	"strings"
)

// Config defines a normalization workflow that converts heterogeneous source
// files into a unified dataset.
type Config struct {
	Paths              []string
	Workers            int
	LogPath            string
	ApprovedOutputRoot string
	OutputPath         string
	MappingFile        string
	RequireMappings    bool
	StatsOutputPath    string
}

// Validate checks whether the ingest configuration is complete enough to run.
func (c *Config) Validate() error {
	if len(c.Paths) == 0 {
		return fmt.Errorf("at least one -path is required")
	}
	if c.Workers < 1 {
		return fmt.Errorf("workers must be at least 1")
	}
	if strings.TrimSpace(c.LogPath) == "" {
		return fmt.Errorf("log-path is required")
	}
	if strings.TrimSpace(c.MappingFile) == "" {
		return fmt.Errorf("mapping-file is required")
	}
	if isRemotePath(c.MappingFile) {
		return fmt.Errorf("mapping-file must be a local path")
	}
	if strings.TrimSpace(c.OutputPath) == "" {
		return fmt.Errorf("output-path is required")
	}
	if err := validateOutputPathExtension(c.OutputPath, []string{".csv", ".json", ".ndjson", ".jsonl"}); err != nil {
		return fmt.Errorf("output-path: %w", err)
	}
	if c.StatsOutputPath != "" {
		if err := validateOutputPathExtension(c.StatsOutputPath, []string{".json"}); err != nil {
			return fmt.Errorf("stats-output-path: %w", err)
		}
		if samePath(c.OutputPath, c.StatsOutputPath) {
			return fmt.Errorf("output-path and stats-output-path must be different")
		}
	}

	return nil
}

func validateOutputPathExtension(path string, allowed []string) error {
	extension := strings.ToLower(filepath.Ext(strings.TrimSpace(path)))
	for _, candidate := range allowed {
		if extension == candidate {
			return nil
		}
	}

	return fmt.Errorf(
		"unsupported extension %q (allowed: %s)",
		extension,
		strings.Join(allowed, ", "),
	)
}

func samePath(left, right string) bool {
	return filepath.Clean(strings.TrimSpace(left)) == filepath.Clean(strings.TrimSpace(right))
}
