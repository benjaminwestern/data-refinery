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
	"github.com/benjaminwestern/data-refinery/internal/ingest"
	"github.com/benjaminwestern/data-refinery/internal/rewrite"
	"github.com/benjaminwestern/data-refinery/internal/tui"
)

const (
	appName         = "data-refinery"
	analysisLogFile = "data-refinery.log"
	ingestLogFile   = "ingest.log"
	rewriteLogFile  = "rewrite.log"
	commandIngest   = "ingest"
	commandRewrite  = "rewrite"
	commandAnalysis = "analysis"
)

// Run dispatches to the analysis or rewrite flow for the current invocation.
func Run(ctx context.Context, args []string) int {
	if len(args) > 0 {
		switch args[0] {
		case commandIngest, "normalize", "normalise":
			if len(args) > 1 && isHelpArg(args[1]) {
				printIngestUsage()
				return 0
			}
			return runWithConfig(ctx, commandIngest, args[1:])
		case commandRewrite:
			if len(args) > 1 && isHelpArg(args[1]) {
				printRewriteUsage()
				return 0
			}
			return runWithConfig(ctx, commandRewrite, args[1:])
		case "analyse", "analyze", commandAnalysis:
			if len(args) > 1 && isHelpArg(args[1]) {
				printAnalysisUsage()
				return 0
			}
			return runWithConfig(ctx, commandAnalysis, args[1:])
		case "help", "-h", "--help":
			if len(args) > 1 {
				return printCommandUsage(args[1])
			}
			printRootUsage()
			return 0
		}
	}

	return runWithConfig(ctx, commandAnalysis, args)
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
	case commandIngest:
		return runIngest(ctx, cfg, safetyOptions, args)
	case commandRewrite:
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
	fs.Usage = printAnalysisUsage
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
	fs.BoolVar(&cfg.EnableJSONOutput, "output.json", cfg.EnableJSONOutput, "Enable .json report output")
	fs.BoolVar(&cfg.PurgeIDs, "purge-ids", cfg.PurgeIDs, "Enable interactive purging of duplicate IDs (local files only)")
	fs.BoolVar(&cfg.PurgeRows, "purge-rows", cfg.PurgeRows, "Enable interactive purging of duplicate rows (local files only)")
	fs.BoolVar(&isHeadless, "headless", false, "Run without TUI and print report to stdout")
	fs.BoolVar(&isValidate, "validate", false, "Run a key validation test and exit (headless only)")
	fs.StringVar(&outputFormat, "output", "txt", "Output format for headless mode (txt or json)")

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
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
			EnableJSONOutput:    cfg.EnableJSONOutput,
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
	fs.Usage = printRewriteUsage
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
		if err == flag.ErrHelp {
			return 0
		}
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

func runIngest(ctx context.Context, baseCfg *config.Config, safetyOptions runtimeSafetyOptions, args []string) int {
	cfg := ingest.Config{
		Workers:            baseCfg.Workers,
		LogPath:            baseCfg.LogPath,
		ApprovedOutputRoot: baseCfg.ApprovedOutputRoot,
	}

	configPath := extractFlagValue(args, "-config")
	if configPath == "" {
		configPath = extractFlagValue(args, "--config")
	}
	if configPath != "" {
		fileCfg, err := ingest.LoadConfigFile(configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading ingest config: %v\n", err)
			return 1
		}
		ingest.MergeConfig(&cfg, fileCfg)
	}

	var pathsInput string
	var configFlag string
	var appConfigFlag string
	var allowImplicitConfig bool
	var unsafeBypass bool

	fs := flag.NewFlagSet("ingest", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = printIngestUsage
	fs.StringVar(&appConfigFlag, "app-config", baseCfg.LoadedConfigPath, "Path to the base app JSON config file")
	fs.BoolVar(&allowImplicitConfig, "allow-implicit-config", safetyOptions.allowImplicitConfig, "Allow implicitly discovered app config files for ingest output paths")
	fs.BoolVar(&unsafeBypass, "yes-i-know-what-im-doing", safetyOptions.unsafeBypass, "Bypass mutation safety checks for config trust and local output roots")
	fs.StringVar(&configFlag, "config", configPath, "Path to a portable ingest JSON config file")
	fs.StringVar(&pathsInput, "path", strings.Join(cfg.Paths, ","), "Comma-separated list of local or GCS paths to normalize")
	fs.IntVar(&cfg.Workers, "workers", cfg.Workers, "Number of concurrent ingest workers")
	fs.StringVar(&cfg.LogPath, "log-path", cfg.LogPath, "Directory to save ingest logs")
	fs.StringVar(&cfg.ApprovedOutputRoot, "approved-output-root", cfg.ApprovedOutputRoot, "Approved local root for ingest output paths (defaults to the current working directory)")
	fs.StringVar(&cfg.OutputPath, "output-path", cfg.OutputPath, "Unified output path (.csv, .json, .ndjson, or .jsonl; local or gs://)")
	fs.StringVar(&cfg.MappingFile, "mapping-file", cfg.MappingFile, "YAML or JSON mapping file that defines schema normalization rules")
	fs.BoolVar(&cfg.RequireMappings, "require-mappings", cfg.RequireMappings, "Fail the run when a discovered file does not match any mapping")
	fs.StringVar(&cfg.StatsOutputPath, "stats-output-path", cfg.StatsOutputPath, "Optional JSON summary path (local or gs://)")

	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}

	cfg.Paths = splitPaths(pathsInput)

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}

	if err := enforceTrustedMutationConfig("ingest", baseCfg, allowImplicitConfig, unsafeBypass); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}

	resolvedRoot, localWriteTargets, err := resolveLocalWriteTargets(cfg.ApprovedOutputRoot, unsafeBypass, collectIngestLocalWriteTargets(cfg))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v. Use --approved-output-root or --yes-i-know-what-im-doing.\n", err)
		return 1
	}

	printMutationSafetySummary(
		"ingest",
		baseCfg,
		[]string{normalizeConfigPath(configFlag)},
		resolvedRoot,
		unsafeBypass,
		localWriteTargets,
		collectIngestRemoteWriteTargets(cfg),
		nil,
		nil,
	)

	if err := os.MkdirAll(cfg.LogPath, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create log directory at %s: %v\n", cfg.LogPath, err)
		return 1
	}

	logFilePath := filepath.Join(cfg.LogPath, ingestLogFile)
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open log file at %s: %v\n", logFilePath, err)
		return 1
	}
	defer closeLogFile(logFile, logFilePath)
	log.SetOutput(logFile)

	summary, err := ingest.Run(ctx, &cfg)
	if summary != nil {
		fmt.Printf("Ingest complete.\n")
		fmt.Printf("Files discovered: %d\n", summary.FilesDiscovered)
		fmt.Printf("Files processed: %d\n", summary.FilesProcessed)
		if summary.FilesSkipped > 0 {
			fmt.Printf("Files skipped: %d\n", summary.FilesSkipped)
		}
		if summary.FilesFailed > 0 {
			fmt.Printf("Files failed: %d\n", summary.FilesFailed)
		}
		fmt.Printf("Rows written: %d\n", summary.RowsWritten)
		fmt.Printf("Unified output: %s\n", summary.OutputPath)
		if summary.StatsOutputPath != "" {
			fmt.Printf("Ingest summary: %s\n", summary.StatsOutputPath)
		}
		fmt.Printf("Ingest log: %s\n", logFilePath)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Ingest failed: %v\n", err)
		return 1
	}

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
	fmt.Printf("%s is a workflow-first CLI for normalization, read-only analysis,\n", appName)
	fmt.Printf("and preview-before-apply cleanup across local storage and GCS.\n\n")
	fmt.Printf("Usage:\n")
	fmt.Printf("  %s [analysis flags]\n", appName)
	fmt.Printf("  %s analyse [analysis flags]\n", appName)
	fmt.Printf("  %s ingest [ingest flags]\n", appName)
	fmt.Printf("  %s rewrite [rewrite flags]\n", appName)
	fmt.Printf("  %s help <analysis|ingest|rewrite>\n\n", appName)
	fmt.Printf("Workflows:\n")
	fmt.Printf("  analysis  Inspect JSON-family datasets, validate keys, review duplicates,\n")
	fmt.Printf("            and generate reports or derived cleanup artifacts.\n")
	fmt.Printf("  ingest    Normalize CSV, TSV, XLSX, JSON, NDJSON, or JSONL inputs into\n")
	fmt.Printf("            one unified dataset with a stable schema.\n")
	fmt.Printf("  rewrite   Preview or apply streamed cleanup rules to JSON, NDJSON, and\n")
	fmt.Printf("            JSONL files with backup-aware apply mode.\n\n")
	fmt.Printf("First useful commands:\n")
	fmt.Printf("  %s ingest -config examples/ingest-simple-config.json\n", appName)
	fmt.Printf("  %s -validate -path ./test_data -key id\n", appName)
	fmt.Printf("  %s rewrite -config examples/rewrite-delete-config.json\n\n", appName)
	fmt.Printf("Supported interface:\n")
	fmt.Printf("  The supported public surface is the CLI plus the JSON or YAML config\n")
	fmt.Printf("  files and examples in this repository. Packages under internal/ are\n")
	fmt.Printf("  implementation details and can change between releases.\n\n")
	fmt.Printf("Help:\n")
	fmt.Printf("  %s ingest --help\n", appName)
	fmt.Printf("  %s analyse --help\n", appName)
	fmt.Printf("  %s rewrite --help\n", appName)
}

type helpFlag struct {
	Name        string
	Value       string
	Description string
}

func printAnalysisUsage() {
	fmt.Printf("analysis inspects JSON, NDJSON, and JSONL datasets without mutating\n")
	fmt.Printf("source data unless you explicitly enable local purge flows.\n\n")
	fmt.Printf("Usage:\n")
	fmt.Printf("  %s [analysis flags]\n", appName)
	fmt.Printf("  %s analyse [analysis flags]\n", appName)
	fmt.Printf("  %s analyze [analysis flags]\n", appName)
	fmt.Printf("  %s analysis [analysis flags]\n\n", appName)
	fmt.Printf("Examples:\n")
	fmt.Printf("  %s -validate -path ./test_data -key id\n", appName)
	fmt.Printf("  %s -headless -path ./test_data -key id -output json\n", appName)
	fmt.Printf("  %s --app-config examples/test_full_advanced.json -headless\n\n", appName)
	fmt.Printf("Outputs:\n")
	fmt.Printf("  analysis always prints a report to stdout and can also write text or\n")
	fmt.Printf("  JSON reports under -log-path. Advanced configs can add search results,\n")
	fmt.Printf("  schema reports, and derived cleanup artifacts.\n")
	printHelpFlags("Core flags", []helpFlag{
		{Name: "path", Value: "PATHS", Description: "Comma-separated list of local paths or gs:// URIs to analyse."},
		{Name: "key", Value: "FIELD", Description: "Top-level key used for duplicate and validation checks."},
		{Name: "workers", Value: "N", Description: "Number of concurrent workers. Default: 8."},
		{Name: "headless", Description: "Run without the TUI and print the report to stdout."},
		{Name: "validate", Description: "Run a key validation pass and exit."},
		{Name: "output", Value: "txt|json", Description: "Stdout format for headless mode."},
		{Name: "check.key", Description: "Enable duplicate-key detection."},
		{Name: "check.row", Description: "Enable duplicate-row hashing."},
		{Name: "show.folders", Description: "Show per-folder breakdowns in the summary output."},
		{Name: "output.txt", Description: "Write text reports under -log-path."},
		{Name: "output.json", Description: "Write JSON reports under -log-path."},
	})
	printHelpFlags("Safety and runtime flags", []helpFlag{
		{Name: "purge-ids", Description: "Enable interactive duplicate-ID purge for local files only."},
		{Name: "purge-rows", Description: "Enable interactive duplicate-row purge for local files only."},
		{Name: "log-path", Value: "DIR", Description: "Directory used for logs and saved reports. Default: logs."},
		{Name: "app-config", Value: "FILE", Description: "Explicit base app config file for advanced analysis and guarded runs."},
		{Name: "allow-implicit-config", Description: "Trust an implicitly discovered base app config for guarded workflows."},
		{Name: "approved-output-root", Value: "DIR", Description: "Approved local root for guarded output paths."},
		{Name: "yes-i-know-what-im-doing", Description: "Bypass config-trust and approved-output-root safety guards."},
	})
}

func printIngestUsage() {
	fmt.Printf("ingest normalizes mixed source formats into one unified dataset that\n")
	fmt.Printf("analysis and rewrite can consume directly.\n\n")
	fmt.Printf("Usage:\n")
	fmt.Printf("  %s ingest [ingest flags]\n", appName)
	fmt.Printf("  %s normalize [ingest flags]\n", appName)
	fmt.Printf("  %s normalise [ingest flags]\n\n", appName)
	fmt.Printf("Examples:\n")
	fmt.Printf("  %s ingest -config examples/ingest-simple-config.json\n", appName)
	fmt.Printf("  %s ingest -config examples/ingest-complex-config.json\n", appName)
	fmt.Printf("  %s ingest -path ./raw,gs://example-bucket/dropbox \\\n", appName)
	fmt.Printf("      -mapping-file ./mappings.yaml \\\n")
	fmt.Printf("      -output-path ./logs/unified.ndjson\n\n")
	fmt.Printf("Supported input formats:\n")
	fmt.Printf("  csv, tsv, xlsx, json, ndjson, jsonl\n\n")
	fmt.Printf("Supported output formats:\n")
	fmt.Printf("  .csv, .json, .ndjson, .jsonl for the normalized dataset, plus optional\n")
	fmt.Printf("  .json run summaries. CSV export is scalar-only. If a row contains nested\n")
	fmt.Printf("  data such as Attributes, the run fails and advises json, ndjson, or jsonl.\n")
	printHelpFlags("Core flags", []helpFlag{
		{Name: "config", Value: "FILE", Description: "Portable ingest job definition. Relative paths resolve from this file."},
		{Name: "path", Value: "PATHS", Description: "Comma-separated list of local paths or gs:// URIs to normalize."},
		{Name: "mapping-file", Value: "FILE", Description: "Local YAML or JSON mapping file that defines normalization rules."},
		{Name: "output-path", Value: "FILE", Description: "Unified output target ending in .csv, .json, .ndjson, or .jsonl."},
		{Name: "stats-output-path", Value: "FILE", Description: "Optional JSON summary target for run statistics."},
		{Name: "require-mappings", Description: "Fail if any discovered file does not match a mapping rule."},
		{Name: "workers", Value: "N", Description: "Number of concurrent ingest workers. Default: 8."},
	})
	printHelpFlags("Safety and runtime flags", []helpFlag{
		{Name: "log-path", Value: "DIR", Description: "Directory used for ingest logs. Default: logs."},
		{Name: "app-config", Value: "FILE", Description: "Explicit base app config file for shared runtime settings."},
		{Name: "allow-implicit-config", Description: "Trust an implicitly discovered base app config for ingest writes."},
		{Name: "approved-output-root", Value: "DIR", Description: "Approved local root for ingest outputs and logs."},
		{Name: "yes-i-know-what-im-doing", Description: "Bypass config-trust and approved-output-root safety guards."},
	})
}

func printRewriteUsage() {
	fmt.Printf("rewrite applies streamed cleanup rules to JSON, NDJSON, and JSONL\n")
	fmt.Printf("datasets. Use preview first, then switch to apply once the summary is\n")
	fmt.Printf("correct.\n\n")
	fmt.Printf("Usage:\n")
	fmt.Printf("  %s rewrite [rewrite flags]\n\n", appName)
	fmt.Printf("Examples:\n")
	fmt.Printf("  %s rewrite -config examples/rewrite-delete-config.json\n", appName)
	fmt.Printf("  %s rewrite -config examples/rewrite-update-config.json\n", appName)
	fmt.Printf("  %s rewrite -path ./test_data -top-level-key customer_id \\\n", appName)
	fmt.Printf("      -top-level-vals ./ids.csv -mode preview\n\n")
	fmt.Printf("Operations:\n")
	fmt.Printf("  delete whole rows by top-level key, delete matching items from nested\n")
	fmt.Printf("  arrays, or recursively update values with optional ID and state filters.\n")
	printHelpFlags("Core flags", []helpFlag{
		{Name: "config", Value: "FILE", Description: "Portable rewrite job definition. Relative paths resolve from this file."},
		{Name: "path", Value: "PATHS", Description: "Comma-separated list of local paths or gs:// URIs to rewrite."},
		{Name: "mode", Value: "preview|apply", Description: "Preview reports changes. Apply writes results and creates backups."},
		{Name: "top-level-key", Value: "FIELD", Description: "Delete whole rows when this top-level key matches."},
		{Name: "top-level-vals", Value: "VALUES|CSV", Description: "Comma-separated match values or a CSV file path."},
		{Name: "array-key", Value: "FIELD", Description: "Array field to inspect for nested deletions."},
		{Name: "array-del-key", Value: "FIELD", Description: "Nested array item key to match for deletion."},
		{Name: "array-del-vals", Value: "VALUES|CSV", Description: "Comma-separated values or a CSV file path for nested deletions."},
		{Name: "state-key", Value: "FIELD", Description: "Optional row-level state filter for delete rules."},
		{Name: "state-value", Value: "VALUE", Description: "Required when -state-key is set."},
		{Name: "update-key", Value: "FIELD", Description: "Key to update recursively."},
		{Name: "update-old-value", Value: "VALUE", Description: "Existing value to replace."},
		{Name: "update-new-value", Value: "VALUE", Description: "Replacement value to write."},
		{Name: "update-id-key", Value: "FIELD", Description: "Optional row filter key for updates."},
		{Name: "update-id-vals", Value: "VALUES|CSV", Description: "Comma-separated values or a CSV file path for selective updates."},
		{Name: "update-state-key", Value: "FIELD", Description: "Optional state filter key for updates."},
		{Name: "update-state-value", Value: "VALUE", Description: "Required when -update-state-key is set."},
		{Name: "workers", Value: "N", Description: "Number of concurrent rewrite workers. Default: 8."},
	})
	printHelpFlags("Safety and runtime flags", []helpFlag{
		{Name: "backup-dir", Value: "DIR", Description: "Backup directory used by apply mode."},
		{Name: "log-path", Value: "DIR", Description: "Directory used for rewrite logs. Default: logs."},
		{Name: "app-config", Value: "FILE", Description: "Explicit base app config file for shared runtime settings."},
		{Name: "allow-implicit-config", Description: "Trust an implicitly discovered base app config for apply mode."},
		{Name: "approved-output-root", Value: "DIR", Description: "Approved local root for apply-mode logs and backups."},
		{Name: "yes-i-know-what-im-doing", Description: "Bypass config-trust and approved-output-root safety guards."},
	})
}

func printHelpFlags(title string, flags []helpFlag) {
	fmt.Printf("\n%s:\n", title)
	for _, flag := range flags {
		printHelpFlag(flag)
	}
}

func printHelpFlag(flagInfo helpFlag) {
	label := "-" + flagInfo.Name
	if flagInfo.Value != "" {
		label += " <" + flagInfo.Value + ">"
	}

	fmt.Printf("  %-42s %s\n", label, flagInfo.Description)
}

func printCommandUsage(command string) int {
	switch strings.ToLower(command) {
	case "analyse", "analyze", "analysis":
		printAnalysisUsage()
		return 0
	case "ingest", "normalize", "normalise":
		printIngestUsage()
		return 0
	case "rewrite":
		printRewriteUsage()
		return 0
	default:
		fmt.Fprintf(os.Stderr, "Unknown command %q.\n\n", command)
		printRootUsage()
		return 1
	}
}

func isHelpArg(arg string) bool {
	switch arg {
	case "help", "-h", "--help":
		return true
	default:
		return false
	}
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
