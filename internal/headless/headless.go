// Package headless runs analysis workflows without the interactive TUI.
package headless

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/benjaminwestern/data-refinery/internal/analyser"
	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/output"
	"github.com/benjaminwestern/data-refinery/internal/report"
	"github.com/benjaminwestern/data-refinery/internal/source"
)

// Config captures the inputs required for a non-interactive analysis run.
type Config struct {
	AppConfig           *config.Config
	Paths               string
	Key                 string
	Workers             int
	LogPath             string
	OutputFormat        string
	ValidateOnly        bool
	CheckKey            bool
	CheckRow            bool
	ShowFolderBreakdown bool
	EnableTxtOutput     bool
	EnableJSONOutput    bool
}

// Run executes an analysis run without the TUI and writes results to stdout.
func Run(ctx context.Context, cfg *Config) {
	if cfg.ValidateOnly {
		fmt.Println("Running in Key Validation Mode...")
	} else {
		fmt.Println("Running in headless mode...")
	}
	startTime := time.Now()

	pathStrings := strings.Split(cfg.Paths, ",")
	for i, p := range pathStrings {
		pathStrings[i] = strings.TrimSpace(p)
	}

	sources, err := source.DiscoverAll(ctx, pathStrings)
	if err != nil {
		fmt.Printf("Error discovering sources: %v\n", err)
		return
	}
	fmt.Printf("Discovered %d files to analyse across %d path(s).\n", len(sources), len(pathStrings))

	// Create unified config for the analyser using the already-loaded application config.
	var analyserConfig config.Config
	if cfg.AppConfig != nil {
		analyserConfig = *cfg.AppConfig
	}
	analyserConfig.Path = cfg.Paths
	analyserConfig.Key = cfg.Key
	analyserConfig.Workers = cfg.Workers
	analyserConfig.LogPath = cfg.LogPath
	analyserConfig.CheckKey = cfg.CheckKey
	analyserConfig.CheckRow = cfg.CheckRow
	analyserConfig.ValidateOnly = cfg.ValidateOnly
	analyserConfig.IsValidationRun = cfg.ValidateOnly
	analyserConfig.ShowFolderBreakdown = cfg.ShowFolderBreakdown
	analyserConfig.EnableTxtOutput = cfg.EnableTxtOutput
	analyserConfig.EnableJSONOutput = cfg.EnableJSONOutput

	eng, err := analyser.New(&analyserConfig)
	if err != nil {
		fmt.Printf("Error creating analyser: %v\n", err)
		return
	}

	finalReport := eng.Run(ctx, sources)

	finalReport.Summary.TotalElapsedTime = time.Since(startTime).Round(time.Second).String()
	filenameBase := report.SaveAndLog(finalReport, cfg.LogPath, cfg.EnableTxtOutput, cfg.EnableJSONOutput, cfg.CheckKey, cfg.CheckRow, cfg.ShowFolderBreakdown)
	if err := output.WriteAdvancedArtifacts(cfg.LogPath, &analyserConfig, finalReport); err != nil {
		fmt.Printf("Warning: failed to write advanced output files: %v\n", err)
	}

	if !cfg.ValidateOnly && (cfg.EnableTxtOutput || cfg.EnableJSONOutput) {
		var parts []string
		if cfg.EnableTxtOutput {
			parts = append(parts, ".txt")
		}
		if cfg.EnableJSONOutput {
			parts = append(parts, ".json")
		}
		fmt.Printf("Analysis complete. Reports saved with base name '%s' and extension(s): %s\n", filenameBase, strings.Join(parts, ", "))
	} else if !cfg.ValidateOnly {
		fmt.Println("Analysis complete. No report files were generated as per configuration.")
	}

	if cfg.OutputFormat == "json" {
		jsonReport, _ := finalReport.ToJSON()
		fmt.Println(jsonReport)
	} else {
		fmt.Println("\n" + finalReport.String(true, cfg.CheckKey, cfg.CheckRow, cfg.ShowFolderBreakdown))
	}
}
