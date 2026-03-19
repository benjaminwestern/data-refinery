package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ResolveApprovedOutputRoot returns the absolute local root used to validate
// guarded mutation write targets. When unset, the current working directory is
// used as the default boundary.
func ResolveApprovedOutputRoot(configuredRoot string) (string, error) {
	root := strings.TrimSpace(configuredRoot)
	if root == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("resolve current working directory: %w", err)
		}
		root = cwd
	}

	if strings.HasPrefix(root, "gs://") {
		return "", fmt.Errorf("approved output root must be a local path, got %q", root)
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve approved output root: %w", err)
	}

	return filepath.Clean(absRoot), nil
}

// ValidateLocalWriteTargets ensures every supplied local target remains under
// the approved local output root. Returned targets are absolute, de-duplicated,
// and preserve first-seen order.
func ValidateLocalWriteTargets(approvedRoot string, targets []string) ([]string, error) {
	absRoot, err := ResolveApprovedOutputRoot(approvedRoot)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(targets))
	resolved := make([]string, 0, len(targets))

	for _, target := range targets {
		trimmed := strings.TrimSpace(target)
		if trimmed == "" {
			continue
		}

		absTarget, err := filepath.Abs(trimmed)
		if err != nil {
			return nil, fmt.Errorf("resolve local write target %q: %w", trimmed, err)
		}
		absTarget = filepath.Clean(absTarget)

		rel, err := filepath.Rel(absRoot, absTarget)
		if err != nil {
			return nil, fmt.Errorf("compare local write target %q to approved root %q: %w", absTarget, absRoot, err)
		}
		if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			return nil, fmt.Errorf("local write target %q escapes approved output root %q", absTarget, absRoot)
		}

		if _, exists := seen[absTarget]; exists {
			continue
		}

		seen[absTarget] = struct{}{}
		resolved = append(resolved, absTarget)
	}

	return resolved, nil
}

// ConfigSourceSummary returns a short description of where the base app config
// came from for operator-facing safety messages.
func (c *Config) ConfigSourceSummary() string {
	if c == nil || c.LoadedConfigPath == "" {
		return "defaults and environment only"
	}
	if c.ConfigLoadedImplicitly {
		return fmt.Sprintf("%s (implicit)", c.LoadedConfigPath)
	}
	return fmt.Sprintf("%s (explicit)", c.LoadedConfigPath)
}
