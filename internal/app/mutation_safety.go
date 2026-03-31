package app

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/ingest"
	"github.com/benjaminwestern/data-refinery/internal/rewrite"
	"github.com/benjaminwestern/data-refinery/internal/safety"
)

type runtimeSafetyOptions struct {
	appConfigPath       string
	allowImplicitConfig bool
	unsafeBypass        bool
}

func parseRuntimeSafetyOptions(args []string) runtimeSafetyOptions {
	return runtimeSafetyOptions{
		appConfigPath: extractAppConfigPath(args),
	}
}

func extractAppConfigPath(args []string) string {
	configPath := extractFlagValue(args, "-app-config")
	if configPath == "" {
		configPath = extractFlagValue(args, "--app-config")
	}
	return configPath
}

func enforceTrustedMutationConfig(command string, cfg *config.Config, allowImplicitConfig, unsafeBypass bool) error {
	if unsafeBypass || cfg == nil || cfg.LoadedConfigPath == "" || !cfg.ConfigLoadedImplicitly || allowImplicitConfig {
		return nil
	}

	return fmt.Errorf(
		"%s is using implicitly discovered app config %s; rerun with --app-config, --allow-implicit-config, or --yes-i-know-what-im-doing",
		command,
		cfg.LoadedConfigPath,
	)
}

func resolveLocalWriteTargets(approvedRoot string, unsafeBypass bool, targets []string) (string, []string, error) {
	resolvedRoot, err := config.ResolveApprovedOutputRoot(approvedRoot)
	if err != nil {
		return "", nil, safety.Wrap(err, "resolve approved output root")
	}

	resolvedTargets := make([]string, 0, len(targets))
	for _, target := range targets {
		trimmed := strings.TrimSpace(target)
		if trimmed == "" {
			continue
		}

		absTarget, err := filepath.Abs(trimmed)
		if err != nil {
			return "", nil, fmt.Errorf("resolve local write target %q: %w", trimmed, err)
		}
		resolvedTargets = append(resolvedTargets, filepath.Clean(absTarget))
	}

	if unsafeBypass {
		return resolvedRoot, dedupeStrings(resolvedTargets), nil
	}

	validatedTargets, err := config.ValidateLocalWriteTargets(resolvedRoot, targets)
	if err != nil {
		return "", nil, safety.Wrap(err, "validate local write targets")
	}

	return resolvedRoot, validatedTargets, nil
}

func printMutationSafetySummary(command string, cfg *config.Config, extraConfigPaths []string, approvedRoot string, bypass bool, localWriteTargets, remoteWriteTargets, localMutationTargets, remoteMutationTargets []string) {
	fmt.Fprintf(os.Stderr, "%s safety preflight:\n", command)
	fmt.Fprintf(os.Stderr, "  app config: %s\n", cfg.ConfigSourceSummary())
	for _, extraConfigPath := range dedupeStrings(extraConfigPaths) {
		if extraConfigPath == "" {
			continue
		}
		fmt.Fprintf(os.Stderr, "  job config: %s\n", extraConfigPath)
	}
	fmt.Fprintf(os.Stderr, "  approved local output root: %s\n", approvedRoot)
	if bypass {
		fmt.Fprintf(os.Stderr, "  safety bypass: enabled via --yes-i-know-what-im-doing\n")
	}
	for _, target := range dedupeStrings(localWriteTargets) {
		fmt.Fprintf(os.Stderr, "  local write target: %s\n", target)
	}
	for _, target := range dedupeStrings(remoteWriteTargets) {
		fmt.Fprintf(os.Stderr, "  remote write target: %s\n", target)
	}
	for _, target := range dedupeStrings(localMutationTargets) {
		fmt.Fprintf(os.Stderr, "  local mutation target: %s\n", target)
	}
	for _, target := range dedupeStrings(remoteMutationTargets) {
		fmt.Fprintf(os.Stderr, "  remote mutation target: %s\n", target)
	}
}

func analysisUsesGuardedWrites(cfg *config.Config) bool {
	return cfg != nil && (cfg.PurgeIDs || cfg.PurgeRows || hasDeletionOutputTargets(cfg))
}

func hasDeletionOutputTargets(cfg *config.Config) bool {
	if cfg == nil || cfg.Advanced == nil {
		return false
	}

	for _, rule := range cfg.Advanced.DeletionRules {
		if strings.TrimSpace(rule.OutputPath) != "" {
			return true
		}
	}

	return false
}

func collectAnalysisLocalWriteTargets(cfg *config.Config) []string {
	if cfg == nil {
		return nil
	}

	targets := []string{cfg.LogPath}
	if cfg.PurgeIDs || cfg.PurgeRows {
		targets = append(targets, "deleted_records")
	}
	if cfg.Advanced != nil {
		for _, rule := range cfg.Advanced.DeletionRules {
			if rule.OutputPath != "" && !strings.HasPrefix(rule.OutputPath, "gs://") {
				targets = append(targets, rule.OutputPath)
			}
		}
	}

	return dedupeStrings(targets)
}

func collectAnalysisRemoteWriteTargets(cfg *config.Config) []string {
	if cfg == nil || cfg.Advanced == nil {
		return nil
	}

	var targets []string
	for _, rule := range cfg.Advanced.DeletionRules {
		if strings.HasPrefix(rule.OutputPath, "gs://") {
			targets = append(targets, rule.OutputPath)
		}
	}

	return dedupeStrings(targets)
}

func collectAnalysisMutationTargets(cfg *config.Config) ([]string, []string) {
	if cfg == nil || (!cfg.PurgeIDs && !cfg.PurgeRows) {
		return nil, nil
	}

	var localTargets []string
	var remoteTargets []string
	for _, path := range splitPaths(cfg.Path) {
		if strings.HasPrefix(path, "gs://") {
			remoteTargets = append(remoteTargets, path)
			continue
		}
		localTargets = append(localTargets, path)
	}

	return dedupeStrings(localTargets), dedupeStrings(remoteTargets)
}

func collectRewriteLocalWriteTargets(cfg rewrite.Config) []string {
	return dedupeStrings([]string{cfg.LogPath, cfg.BackupDir})
}

func collectRewriteMutationTargets(cfg rewrite.Config) ([]string, []string) {
	var localTargets []string
	var remoteTargets []string

	for _, path := range cfg.Paths {
		if strings.HasPrefix(path, "gs://") {
			remoteTargets = append(remoteTargets, path)
			continue
		}
		localTargets = append(localTargets, path)
	}

	return dedupeStrings(localTargets), dedupeStrings(remoteTargets)
}

func collectIngestLocalWriteTargets(cfg ingest.Config) []string {
	targets := []string{cfg.LogPath}
	if cfg.OutputPath != "" && !strings.HasPrefix(cfg.OutputPath, "gs://") {
		targets = append(targets, cfg.OutputPath)
	}
	if cfg.StatsOutputPath != "" && !strings.HasPrefix(cfg.StatsOutputPath, "gs://") {
		targets = append(targets, cfg.StatsOutputPath)
	}

	return dedupeStrings(targets)
}

func collectIngestRemoteWriteTargets(cfg ingest.Config) []string {
	var targets []string
	if strings.HasPrefix(cfg.OutputPath, "gs://") {
		targets = append(targets, cfg.OutputPath)
	}
	if strings.HasPrefix(cfg.StatsOutputPath, "gs://") {
		targets = append(targets, cfg.StatsOutputPath)
	}

	return dedupeStrings(targets)
}

func normalizeConfigPath(path string) string {
	if path == "" {
		return ""
	}
	if absPath, err := filepath.Abs(path); err == nil {
		return filepath.Clean(absPath)
	}
	return path
}

func dedupeStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	deduped := make([]string, 0, len(values))

	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		deduped = append(deduped, trimmed)
	}

	return deduped
}
