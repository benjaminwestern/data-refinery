// Package app wires the CLI entrypoints for analysis and rewrite commands.
package app

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/headless"
	"github.com/benjaminwestern/data-refinery/internal/rewrite"
	"github.com/benjaminwestern/data-refinery/internal/tui"
)

const (
	appName         = "data-refinery"
	analysisLogFile = "data-refinery.log"
	rewriteLogFile  = "rewrite.log"
)

// Run dispatches to the analysis or rewrite flow for the current invocation.
func Run(ctx context.Context, args []string) int {
	if len(args) > 0 {
		switch args[0] {
		case "rewrite":
			return runWithConfig(ctx, "rewrite", args[1:])
		case "analyse", "analyze", "analysis":
			return runWithConfig(ctx, "analysis", args[1:])
		case "help", "-h", "--help":
			printRootUsage()
			return 0
		}
	}

	return runWithConfig(ctx, "analysis", args)
}

func runWithConfig(ctx context.Context, command string, args []string) int {
	safetyOptions := parseRuntimeSafetyOptions(args)

	cfg, err := config.LoadWithOptions(config.LoadOptions{
		ExplicitPath:  safetyOptions.appConfigPath,
		AllowImplicit: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		return 1
	}

	switch command {
	case "rewrite":
		return runRewrite(ctx, cfg, safetyOptions, args)
	default:
		return runAnalysis(ctx, cfg, safetyOptions, args)
	}
}

func runAnalysis(ctx context.Context, cfg *config.Config, safetyOptions runtimeSafetyOptions, args []string) int {
	var isHeadless bool
	var isValidate bool
	var outputFormat string
	var keyIsSet bool
	var appConfigFlag string
	var allowImplicitConfig bool
	var unsafeBypass bool

	fs := flag.NewFlagSet("analysis", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&appConfigFlag, "app-config", cfg.LoadedConfigPath, "Path to the base app JSON config file")
	fs.BoolVar(&allowImplicitConfig, "allow-implicit-config", safetyOptions.allowImplicitConfig, "Allow implicitly discovered app config files for guarded mutation flows")
	fs.BoolVar(&unsafeBypass, "yes-i-know-what-im-doing", safetyOptions.unsafeBypass, "Bypass mutation safety checks for config trust and local output roots")
	fs.StringVar(&cfg.Path, "path", cfg.Path, "Comma-separated list of paths to analyse (local or GCS)")
	fs.StringVar(&cfg.Key, "key", cfg.Key, "JSON key for uniqueness check")
	fs.IntVar(&cfg.Workers, "workers", cfg.Workers, "Number of concurrent workers")
	fs.StringVar(&cfg.LogPath, "log-path", cfg.LogPath, "Directory to save logs and reports")
	fs.StringVar(&cfg.ApprovedOutputRoot, "approved-output-root", cfg.ApprovedOutputRoot, "Approved local root for guarded mutation output paths (defaults to the current working directory)")
	fs.BoolVar(&cfg.CheckKey, "check.key", cfg.CheckKey, "Enable duplicate key check")
	fs.BoolVar(&cfg.CheckRow, "check.row", cfg.CheckRow, "Enable duplicate row check (hashing)")
	fs.BoolVar(&cfg.ShowFolderBreakdown, "show.folders", cfg.ShowFolderBreakdown, "Show per-folder breakdown table in summary report")
	fs.BoolVar(&cfg.EnableTxtOutput, "output.txt", cfg.EnableTxtOutput, "Enable .txt report output")
	fs.BoolVar(&cfg.EnableJsonOutput, "output.json", cfg.EnableJsonOutput, "Enable .json report output")
	fs.BoolVar(&cfg.PurgeIDs, "purge-ids", cfg.PurgeIDs, "Enable interactive purging of duplicate IDs (local files only)")
	fs.BoolVar(&cfg.PurgeRows, "purge-rows", cfg.PurgeRows, "Enable interactive purging of duplicate rows (local files only)")
	fs.BoolVar(&isHeadless, "headless", false, "Run without TUI and print report to stdout")
	fs.BoolVar(&isValidate, "validate", false, "Run a key validation test and exit (headless only)")
	fs.StringVar(&outputFormat, "output", "txt", "Output format for headless mode (txt or json)")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	fs.Visit(func(f *flag.Flag) {
		if f.Name == "key" {
			keyIsSet = true
		}
	})

	cfg.AllowImplicitMutationConfig = allowImplicitConfig
	cfg.UnsafeMutationBypass = unsafeBypass

	isGCSPath := strings.Contains(cfg.Path, "gs://")
	if isGCSPath && (cfg.PurgeIDs || cfg.PurgeRows) {
		fmt.Println("Error: Purge functionality is only available for local files, not for GCS paths.")
		return 1
	}

	if !isHeadless && cfg.Path == "" && fs.NArg() > 0 {
		cfg.Path = strings.Join(fs.Args(), ",")
	}

	if isHeadless || isValidate {
		if cfg.Path == "" {
			fmt.Println("Error: -path flag is required for headless/validation mode.")
			return 1
		}
		if cfg.Key == "" {
			fmt.Println("Error: -key flag is required for validation mode.")
			return 1
		}
		if isHeadless && !isValidate && !cfg.CheckKey && !cfg.CheckRow {
			fmt.Println("Error: At least one check (-check.key or -check.row) must be enabled for a full analysis.")
			return 1
		}
		if cfg.CheckKey && !keyIsSet {
			fmt.Println("Warning: -key flag not set, defaulting to 'id'.")
		}
	}

	if !cfg.CheckKey && !cfg.CheckRow {
		fmt.Println("Error: At least one check (-check.key or -check.row) must be enabled.")
		return 1
	}

	if !isValidate && analysisUsesGuardedWrites(cfg) {
		if err := enforceTrustedMutationConfig("analysis", cfg, allowImplicitConfig, unsafeBypass); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		}

		resolvedRoot, localWriteTargets, err := resolveLocalWriteTargets(cfg.ApprovedOutputRoot, unsafeBypass, collectAnalysisLocalWriteTargets(cfg))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v. Use --approved-output-root or --yes-i-know-what-im-doing.\n", err)
			return 1
		}

		localMutationTargets, remoteMutationTargets := collectAnalysisMutationTargets(cfg)
		printMutationSafetySummary(
			"analysis",
			cfg,
			nil,
			resolvedRoot,
			unsafeBypass,
			localWriteTargets,
			collectAnalysisRemoteWriteTargets(cfg),
			localMutationTargets,
			remoteMutationTargets,
		)
	}

	if err := os.MkdirAll(cfg.LogPath, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create log directory at %s: %v\n", cfg.LogPath, err)
		return 1
	}

	logFilePath := filepath.Join(cfg.LogPath, analysisLogFile)
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open log file at %s: %v\n", logFilePath, err)
		return 1
	}
	defer closeLogFile(logFile, logFilePath)
	log.SetOutput(logFile)

	if isHeadless || isValidate {
		headlessCfg := &headless.Config{
			AppConfig:           cfg,
			Paths:               cfg.Path,
			Key:                 cfg.Key,
			Workers:             cfg.Workers,
			LogPath:             cfg.LogPath,
			OutputFormat:        outputFormat,
			ValidateOnly:        isValidate,
			CheckKey:            cfg.CheckKey,
			CheckRow:            cfg.CheckRow,
			ShowFolderBreakdown: cfg.ShowFolderBreakdown,
			EnableTxtOutput:     cfg.EnableTxtOutput,
			EnableJsonOutput:    cfg.EnableJsonOutput,
		}

		headless.Run(ctx, headlessCfg)
		return 0
	}

	currentConfig := cfg
	for {
		finalConfig, shouldRestart, startNew, err := tui.Run(currentConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error running application: %v\n", err)
			return 1
		}
		if !shouldRestart {
			break
		}

		if startNew {
			newCfg, loadErr := config.LoadWithOptions(config.LoadOptions{
				ExplicitPath:  safetyOptions.appConfigPath,
				AllowImplicit: true,
			})
			if loadErr != nil {
				fmt.Fprintf(os.Stderr, "Error reloading configuration for new job: %v\n", loadErr)
				return 1
			}
			newCfg.LogPath = cfg.LogPath
			newCfg.ApprovedOutputRoot = cfg.ApprovedOutputRoot
			newCfg.AllowImplicitMutationConfig = allowImplicitConfig
			newCfg.UnsafeMutationBypass = unsafeBypass
			currentConfig = newCfg
		} else {
			finalConfig.AllowImplicitMutationConfig = allowImplicitConfig
			finalConfig.UnsafeMutationBypass = unsafeBypass
			finalConfig.LoadedConfigPath = cfg.LoadedConfigPath
			finalConfig.ConfigLoadedImplicitly = cfg.ConfigLoadedImplicitly
			currentConfig = finalConfig
		}
	}

	return 0
}

func runRewrite(ctx context.Context, baseCfg *config.Config, safetyOptions runtimeSafetyOptions, args []string) int {
	cfg := rewrite.Config{
		Workers:            baseCfg.Workers,
		LogPath:            baseCfg.LogPath,
		ApprovedOutputRoot: baseCfg.ApprovedOutputRoot,
		Mode:               rewrite.ModePreview,
	}

	if baseCfg.Performance != nil {
		cfg.BufferSize = baseCfg.Performance.BufferSize
		cfg.MaxBufferSize = baseCfg.Performance.MaxBufferSize
	}

	configPath := extractFlagValue(args, "-config")
	if configPath == "" {
		configPath = extractFlagValue(args, "--config")
	}
	if configPath != "" {
		fileCfg, err := rewrite.LoadConfigFile(configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading rewrite config: %v\n", err)
			return 1
		}
		rewrite.MergeConfig(&cfg, fileCfg)
	}

	var pathsInput string
	var topLevelValsInput string
	var arrayDelValsInput string
	var updateIDValsInput string
	var configFlag string
	var appConfigFlag string
	var allowImplicitConfig bool
	var unsafeBypass bool

	fs := flag.NewFlagSet("rewrite", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&appConfigFlag, "app-config", baseCfg.LoadedConfigPath, "Path to the base app JSON config file")
	fs.BoolVar(&allowImplicitConfig, "allow-implicit-config", safetyOptions.allowImplicitConfig, "Allow implicitly discovered app config files for apply-mode rewrites")
	fs.BoolVar(&unsafeBypass, "yes-i-know-what-im-doing", safetyOptions.unsafeBypass, "Bypass mutation safety checks for config trust and local output roots")
	fs.StringVar(&configFlag, "config", configPath, "Path to a portable rewrite JSON config file")
	fs.StringVar(&pathsInput, "path", strings.Join(cfg.Paths, ","), "Comma-separated list of local or GCS paths to rewrite")
	fs.IntVar(&cfg.Workers, "workers", cfg.Workers, "Number of concurrent workers")
	fs.StringVar(&cfg.LogPath, "log-path", cfg.LogPath, "Directory to save rewrite logs")
	fs.StringVar(&cfg.ApprovedOutputRoot, "approved-output-root", cfg.ApprovedOutputRoot, "Approved local root for apply-mode output paths (defaults to the current working directory)")
	fs.StringVar((*string)(&cfg.Mode), "mode", string(cfg.Mode), "Rewrite mode: preview or apply")
	fs.StringVar(&cfg.BackupDir, "backup-dir", cfg.BackupDir, "Directory used for backups in apply mode")
	fs.StringVar(&cfg.TopLevelKey, "top-level-key", cfg.TopLevelKey, "Delete whole rows when this key matches")
	fs.StringVar(&topLevelValsInput, "top-level-vals", strings.Join(cfg.TopLevelValues, ","), "Comma-separated values or CSV file path for top-level row deletion")
	fs.StringVar(&cfg.ArrayKey, "array-key", cfg.ArrayKey, "Array key to rewrite within each row")
	fs.StringVar(&cfg.ArrayDeleteKey, "array-del-key", cfg.ArrayDeleteKey, "Nested array item key to match for deletion")
	fs.StringVar(&arrayDelValsInput, "array-del-vals", strings.Join(cfg.ArrayDeleteValues, ","), "Comma-separated values or CSV file path for nested array deletion")
	fs.StringVar(&cfg.StateKey, "state-key", cfg.StateKey, "Optional state key filter for deletion rules")
	fs.StringVar(&cfg.StateValue, "state-value", cfg.StateValue, "Optional state value filter for deletion rules")
	fs.StringVar(&cfg.UpdateKey, "update-key", cfg.UpdateKey, "Key to update recursively")
	fs.StringVar(&cfg.UpdateOldValue, "update-old-value", cfg.UpdateOldValue, "Old value to replace")
	fs.StringVar(&cfg.UpdateNewValue, "update-new-value", cfg.UpdateNewValue, "New value to write")
	fs.StringVar(&cfg.UpdateIDKey, "update-id-key", cfg.UpdateIDKey, "Optional row filter key for updates")
	fs.StringVar(&updateIDValsInput, "update-id-vals", strings.Join(cfg.UpdateIDValues, ","), "Comma-separated values or CSV file path for selective updates")
	fs.StringVar(&cfg.UpdateStateKey, "update-state-key", cfg.UpdateStateKey, "Optional state key filter for updates")
	fs.StringVar(&cfg.UpdateStateValue, "update-state-value", cfg.UpdateStateValue, "Optional state value filter for updates")

	if err := fs.Parse(args); err != nil {
		return 2
	}

	cfg.Paths = splitPaths(pathsInput)

	var err error
	cfg.TopLevelValues, err = rewrite.ParseValuesInput(topLevelValsInput)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing -top-level-vals: %v\n", err)
		return 1
	}

	cfg.ArrayDeleteValues, err = rewrite.ParseValuesInput(arrayDelValsInput)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing -array-del-vals: %v\n", err)
		return 1
	}

	cfg.UpdateIDValues, err = rewrite.ParseValuesInput(updateIDValsInput)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing -update-id-vals: %v\n", err)
		return 1
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}

	if cfg.Mode == rewrite.ModeApply {
		if err := enforceTrustedMutationConfig("rewrite", baseCfg, allowImplicitConfig, unsafeBypass); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		}

		resolvedRoot, localWriteTargets, err := resolveLocalWriteTargets(cfg.ApprovedOutputRoot, unsafeBypass, collectRewriteLocalWriteTargets(cfg))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v. Use --approved-output-root or --yes-i-know-what-im-doing.\n", err)
			return 1
		}

		localMutationTargets, remoteMutationTargets := collectRewriteMutationTargets(cfg)
		printMutationSafetySummary(
			"rewrite",
			baseCfg,
			[]string{normalizeConfigPath(configFlag)},
			resolvedRoot,
			unsafeBypass,
			localWriteTargets,
			nil,
			localMutationTargets,
			remoteMutationTargets,
		)
	}

	if err := os.MkdirAll(cfg.LogPath, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create log directory at %s: %v\n", cfg.LogPath, err)
		return 1
	}

	logFilePath := filepath.Join(cfg.LogPath, rewriteLogFile)
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open log file at %s: %v\n", logFilePath, err)
		return 1
	}
	defer closeLogFile(logFile, logFilePath)
	log.SetOutput(logFile)

	summary, err := rewrite.Run(ctx, &cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Rewrite failed: %v\n", err)
		return 1
	}

	fmt.Printf("Rewrite complete (%s).\n", summary.Mode)
	fmt.Printf("Files discovered: %d\n", summary.FilesDiscovered)
	fmt.Printf("Files processed: %d\n", summary.FilesProcessed)
	fmt.Printf("Files modified: %d\n", summary.FilesModified)
	fmt.Printf("Lines read: %d\n", summary.LinesRead)
	if summary.LinesDeleted > 0 {
		fmt.Printf("Lines deleted: %d\n", summary.LinesDeleted)
	}
	if summary.LinesModified > 0 {
		fmt.Printf("Lines modified: %d\n", summary.LinesModified)
	}
	if summary.LinesUpdated > 0 {
		fmt.Printf("Lines updated: %d\n", summary.LinesUpdated)
	}
	if summary.Errors > 0 {
		fmt.Printf("Warnings: %d malformed rows were preserved as-is\n", summary.Errors)
	}
	if summary.BackupPath != "" {
		fmt.Printf("Backup snapshot: %s\n", summary.BackupPath)
	}
	fmt.Printf("Rewrite log: %s\n", logFilePath)

	return 0
}

func splitPaths(input string) []string {
	if input == "" {
		return nil
	}

	parts := strings.Split(input, ",")
	paths := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			paths = append(paths, trimmed)
		}
	}
	return paths
}

func closeLogFile(logFile *os.File, logFilePath string) {
	if err := logFile.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to close log file at %s: %v\n", logFilePath, err)
	}
}

func printRootUsage() {
	fmt.Printf("%s combines duplicate analysis with safe JSON/NDJSON rewrite workflows.\n\n", appName)
	fmt.Printf("Usage:\n")
	fmt.Printf("  %s [analysis flags]\n", appName)
	fmt.Printf("  %s rewrite [rewrite flags]\n", appName)
	fmt.Printf("  %s analyse [analysis flags]\n\n", appName)
	fmt.Printf("Examples:\n")
	fmt.Printf("  %s -path /data -key id\n", appName)
	fmt.Printf("  %s rewrite -config examples/rewrite-delete-config.json\n", appName)
}

func extractFlagValue(args []string, flagName string) string {
	for i, arg := range args {
		if arg == flagName && i+1 < len(args) {
			return args[i+1]
		}
		prefix := flagName + "="
		if strings.HasPrefix(arg, prefix) {
			return strings.TrimPrefix(arg, prefix)
		}
	}
	return ""
}
