package output

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/benjaminwestern/dupe-analyser/internal/config"
	"github.com/benjaminwestern/dupe-analyser/internal/deletion"
	"github.com/benjaminwestern/dupe-analyser/internal/report"
	"github.com/benjaminwestern/dupe-analyser/internal/schema"
	"github.com/benjaminwestern/dupe-analyser/internal/search"
	"github.com/benjaminwestern/dupe-analyser/internal/source"
)

// OutputManager manages different types of output files and formats
type OutputManager struct {
	basePath        string
	outputFiles     map[string]*os.File
	outputWriters   map[string]*bufio.Writer
	outputMutex     sync.Mutex
	searchResults   *search.SearchResults
	schemaAnalyzer  *schema.SchemaAnalyzer
	schemaReport    *schema.SchemaReport
	deletionResults *deletion.DeletionStats
	analysisReport  *report.AnalysisReport
	timestamp       string
}

// NewOutputManager creates a new output manager
func NewOutputManager(basePath string) *OutputManager {
	return &OutputManager{
		basePath:      basePath,
		outputFiles:   make(map[string]*os.File),
		outputWriters: make(map[string]*bufio.Writer),
		timestamp:     time.Now().Format("2006-01-02_15-04-05"),
	}
}

// CreateOutputFile creates a new output file with the given name and extension
func (om *OutputManager) CreateOutputFile(name, extension string) (string, error) {
	om.outputMutex.Lock()
	defer om.outputMutex.Unlock()

	filename := fmt.Sprintf("%s_%s.%s", name, om.timestamp, extension)
	fullPath := filepath.Join(om.basePath, filename)

	// Create directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return "", fmt.Errorf("failed to create output file %s: %w", fullPath, err)
	}

	om.outputFiles[name] = file
	om.outputWriters[name] = bufio.NewWriter(file)

	return fullPath, nil
}

// WriteJSONLine writes a JSON line to the specified output file
func (om *OutputManager) WriteJSONLine(fileNameOrPath string, data any) error {
	om.outputMutex.Lock()
	defer om.outputMutex.Unlock()

	// Try to find the writer by key first
	writer, exists := om.outputWriters[fileNameOrPath]
	if !exists {
		// If not found by key, try to extract the key from the file path
		// The key is the base name without extension and timestamp
		baseName := filepath.Base(fileNameOrPath)

		// Remove extension
		nameWithoutExt := strings.TrimSuffix(baseName, filepath.Ext(baseName))

		// Remove timestamp suffix (pattern: _YYYY-MM-DD_HH-MM-SS)
		parts := strings.Split(nameWithoutExt, "_")
		if len(parts) >= 3 {
			// Try to remove the timestamp part (last 3 parts might be timestamp)
			// Format: name_YYYY-MM-DD_HH-MM-SS
			keyName := strings.Join(parts[:len(parts)-2], "_")
			if writer, exists = om.outputWriters[keyName]; exists {
				// Found it
			} else {
				// Try just the first part as key
				if writer, exists = om.outputWriters[parts[0]]; exists {
					// Found it
				}
			}
		}

		if !exists {
			return fmt.Errorf("output file %s not found", fileNameOrPath)
		}
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	if _, err := writer.Write(jsonData); err != nil {
		return fmt.Errorf("failed to write JSON line: %w", err)
	}

	if _, err := writer.WriteString("\n"); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}

	return nil
}

// WriteString writes a string to the specified output file
func (om *OutputManager) WriteString(fileName, content string) error {
	om.outputMutex.Lock()
	defer om.outputMutex.Unlock()

	writer, exists := om.outputWriters[fileName]
	if !exists {
		return fmt.Errorf("output file %s not found", fileName)
	}

	if _, err := writer.WriteString(content); err != nil {
		return fmt.Errorf("failed to write string: %w", err)
	}

	return nil
}

// SetSearchResults sets the search results for output
func (om *OutputManager) SetSearchResults(results *search.SearchResults) {
	om.searchResults = results
}

// SetSchemaAnalyzer sets the schema analyzer for output
func (om *OutputManager) SetSchemaAnalyzer(analyzer *schema.SchemaAnalyzer) {
	om.schemaAnalyzer = analyzer
}

// SetSchemaReport sets a pre-generated schema report for output.
func (om *OutputManager) SetSchemaReport(schemaReport *schema.SchemaReport) {
	om.schemaReport = schemaReport
}

// SetDeletionResults sets the deletion results for output
func (om *OutputManager) SetDeletionResults(results *deletion.DeletionStats) {
	om.deletionResults = results
}

// SetAnalysisReport sets the analysis report for standard output generation.
func (om *OutputManager) SetAnalysisReport(rep *report.AnalysisReport) {
	om.analysisReport = rep
}

func (om *OutputManager) currentSchemaReport() *schema.SchemaReport {
	if om.schemaReport != nil {
		return om.schemaReport
	}
	if om.schemaAnalyzer != nil {
		return om.schemaAnalyzer.GenerateReport()
	}
	return nil
}

// GenerateAllOutputs generates all configured output files
func (om *OutputManager) GenerateAllOutputs(cfg *config.Config) error {
	var errors []error

	// Generate analysis reports
	if cfg.EnableTxtOutput || cfg.EnableJsonOutput {
		if err := om.generateAnalysisReports(cfg); err != nil {
			errors = append(errors, err)
		}
	}

	// Generate search results
	if om.searchResults != nil {
		if err := om.generateSearchResults(); err != nil {
			errors = append(errors, err)
		}
	}

	// Generate schema reports
	if om.schemaAnalyzer != nil && cfg.Advanced != nil && cfg.Advanced.SchemaDiscovery.Enabled {
		if err := om.generateSchemaReports(cfg.Advanced.SchemaDiscovery); err != nil {
			errors = append(errors, err)
		}
	}

	// Generate deletion reports
	if om.deletionResults != nil {
		if err := om.generateDeletionReports(); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("output generation errors: %v", errors)
	}

	return nil
}

// generateAnalysisReports generates standard analysis reports
func (om *OutputManager) generateAnalysisReports(cfg *config.Config) error {
	if om.analysisReport == nil {
		return nil
	}

	if cfg.EnableTxtOutput {
		if _, err := om.CreateOutputFile("analysis_summary", "txt"); err != nil {
			return err
		}
		if err := om.WriteString("analysis_summary", om.analysisReport.String(false, cfg.CheckKey, cfg.CheckRow, cfg.ShowFolderBreakdown)); err != nil {
			return err
		}
		if _, err := om.CreateOutputFile("analysis_details", "txt"); err != nil {
			return err
		}
		if err := om.WriteString("analysis_details", om.analysisReport.String(true, cfg.CheckKey, cfg.CheckRow, cfg.ShowFolderBreakdown)); err != nil {
			return err
		}
	}

	if cfg.EnableJsonOutput {
		if _, err := om.CreateOutputFile("analysis_report", "json"); err != nil {
			return err
		}
		if err := om.WriteJSONLine("analysis_report", om.analysisReport); err != nil {
			return err
		}
	}

	return nil
}

// generateSearchResults generates search results output
func (om *OutputManager) generateSearchResults() error {
	// Generate JSON search results
	_, err := om.CreateOutputFile("search_results", "json")
	if err != nil {
		return err
	}

	if err := om.WriteJSONLine("search_results", om.searchResults); err != nil {
		return err
	}

	// Generate detailed search results by target
	for targetName, results := range om.searchResults.Results {
		if len(results) > 0 {
			safeTargetName := strings.ReplaceAll(targetName, " ", "_")
			targetPath, err := om.CreateOutputFile(fmt.Sprintf("search_target_%s", safeTargetName), "json")
			if err != nil {
				return err
			}

			for _, result := range results {
				if err := om.WriteJSONLine(fmt.Sprintf("search_target_%s", safeTargetName), result); err != nil {
					return err
				}
			}

			fmt.Printf("Generated search results for target '%s': %s\n", targetName, targetPath)
		}
	}

	return nil
}

// generateSchemaReports generates schema discovery reports
func (om *OutputManager) generateSchemaReports(cfg config.SchemaDiscoveryConfig) error {
	schemaReport := om.currentSchemaReport()
	if schemaReport == nil {
		return nil
	}

	for _, format := range cfg.OutputFormats {
		switch strings.ToLower(format) {
		case "json":
			path, err := om.CreateOutputFile("schema_report", "json")
			if err != nil {
				return err
			}
			if err := om.WriteJSONLine("schema_report", schemaReport); err != nil {
				return err
			}
			fmt.Printf("Generated schema report: %s\n", path)

		case "csv":
			path, err := om.CreateOutputFile("schema_report", "csv")
			if err != nil {
				return err
			}
			if err := om.generateSchemaCSVInternal(schemaReport, "schema_report"); err != nil {
				return err
			}
			fmt.Printf("Generated schema CSV: %s\n", path)

		case "yaml":
			path, err := om.CreateOutputFile("schema_report", "yaml")
			if err != nil {
				return err
			}
			if err := om.generateSchemaYAMLInternal(schemaReport, "schema_report"); err != nil {
				return err
			}
			fmt.Printf("Generated schema YAML: %s\n", path)
		}
	}

	return nil
}

// writeSchemaCSVToFile writes schema report to a file in CSV format
func (om *OutputManager) writeSchemaCSVToFile(file *os.File, schemaReport *schema.SchemaReport) error {
	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// Write CSV header
	header := "Scope,Path,Type,Occurrences,Percentage,IsNullable,UniqueCount,MinLength,MaxLength,Examples\n"
	if _, err := writer.WriteString(header); err != nil {
		return err
	}

	// Write global schema
	if err := om.writeSchemaFieldsCSVToWriter(writer, schemaReport.GlobalSchema, "global"); err != nil {
		return err
	}

	// Write folder schemas
	for folderName, schema := range schemaReport.FolderSchemas {
		if err := om.writeSchemaFieldsCSVToWriter(writer, schema, folderName); err != nil {
			return err
		}
	}

	return nil
}

// writeSchemaFieldsCSVToWriter writes schema fields to a CSV writer
func (om *OutputManager) writeSchemaFieldsCSVToWriter(writer *bufio.Writer, schema *schema.Schema, scope string) error {
	for _, field := range schema.Fields {
		examples := make([]string, len(field.Examples))
		for i, ex := range field.Examples {
			examples[i] = fmt.Sprintf("%v", ex)
		}

		line := fmt.Sprintf("%s,%s,%s,%d,%.2f,%t,%d,%d,%d,\"%s\"\n",
			scope,
			field.Path,
			field.Type,
			field.Occurrences,
			field.Percentage,
			field.IsNullable,
			field.UniqueCount,
			field.MinLength,
			field.MaxLength,
			strings.Join(examples, "; "),
		)

		if _, err := writer.WriteString(line); err != nil {
			return err
		}
	}

	return nil
}

// writeSchemaYAMLToFile writes schema report to a file in YAML format
func (om *OutputManager) writeSchemaYAMLToFile(file *os.File, schemaReport *schema.SchemaReport) error {
	writer := bufio.NewWriter(file)
	defer writer.Flush()

	// For now, we'll convert to JSON and then to YAML
	// In a full implementation, you'd use a proper YAML library
	jsonData, err := json.MarshalIndent(schemaReport, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal schema report: %w", err)
	}

	// Simple JSON to YAML conversion (basic)
	yamlContent := om.jsonToYAML(string(jsonData))
	_, err = writer.WriteString(yamlContent)
	return err
}

// generateSchemaCSVInternal generates schema report in CSV format using the writer system
func (om *OutputManager) generateSchemaCSVInternal(schemaReport *schema.SchemaReport, fileName string) error {
	// Write CSV header
	header := "Scope,Path,Type,Occurrences,Percentage,IsNullable,UniqueCount,MinLength,MaxLength,Examples\n"
	if err := om.WriteString(fileName, header); err != nil {
		return err
	}

	// Write global schema
	if err := om.writeSchemaFieldsCSV(schemaReport.GlobalSchema, "global", fileName); err != nil {
		return err
	}

	// Write folder schemas
	for folderName, schema := range schemaReport.FolderSchemas {
		if err := om.writeSchemaFieldsCSV(schema, folderName, fileName); err != nil {
			return err
		}
	}

	return nil
}

// writeSchemaFieldsCSV writes schema fields to CSV using the writer system
func (om *OutputManager) writeSchemaFieldsCSV(schema *schema.Schema, scope string, fileName string) error {
	for _, field := range schema.Fields {
		examples := make([]string, len(field.Examples))
		for i, ex := range field.Examples {
			examples[i] = fmt.Sprintf("%v", ex)
		}

		line := fmt.Sprintf("%s,%s,%s,%d,%.2f,%t,%d,%d,%d,\"%s\"\n",
			scope,
			field.Path,
			field.Type,
			field.Occurrences,
			field.Percentage,
			field.IsNullable,
			field.UniqueCount,
			field.MinLength,
			field.MaxLength,
			strings.Join(examples, "; "),
		)

		if err := om.WriteString(fileName, line); err != nil {
			return err
		}
	}

	return nil
}

// generateSchemaYAMLInternal generates schema report in YAML format using the writer system
func (om *OutputManager) generateSchemaYAMLInternal(schemaReport *schema.SchemaReport, fileName string) error {
	// For now, we'll convert to JSON and then to YAML
	// In a full implementation, you'd use a proper YAML library
	jsonData, err := json.MarshalIndent(schemaReport, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal schema report: %w", err)
	}

	// Simple JSON to YAML conversion (basic)
	yamlContent := om.jsonToYAML(string(jsonData))
	return om.WriteString(fileName, yamlContent)
}

// generateSchemaYAML generates schema report in YAML format
func (om *OutputManager) generateSchemaYAML(schemaReport *schema.SchemaReport) error {
	// For now, we'll convert to JSON and then to YAML
	// In a full implementation, you'd use a proper YAML library
	jsonData, err := json.MarshalIndent(schemaReport, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal schema report: %w", err)
	}

	// Simple JSON to YAML conversion (basic)
	yamlContent := om.jsonToYAML(string(jsonData))
	return om.WriteString("schema_report", yamlContent)
}

// jsonToYAML performs a basic JSON to YAML conversion
func (om *OutputManager) jsonToYAML(jsonStr string) string {
	// This is a very basic conversion - in production you'd use a proper YAML library
	lines := strings.Split(jsonStr, "\n")
	var yamlLines []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		// Remove JSON punctuation and convert to YAML style
		yamlLine := strings.ReplaceAll(trimmed, "\"", "")
		yamlLine = strings.ReplaceAll(yamlLine, ",", "")
		yamlLine = strings.ReplaceAll(yamlLine, "{", "")
		yamlLine = strings.ReplaceAll(yamlLine, "}", "")
		yamlLine = strings.ReplaceAll(yamlLine, "[", "")
		yamlLine = strings.ReplaceAll(yamlLine, "]", "")

		if yamlLine != "" {
			yamlLines = append(yamlLines, yamlLine)
		}
	}

	return strings.Join(yamlLines, "\n")
}

// generateDeletionReports generates deletion operation reports
func (om *OutputManager) generateDeletionReports() error {
	// Generate deletion statistics
	path, err := om.CreateOutputFile("deletion_stats", "json")
	if err != nil {
		return err
	}

	if err := om.WriteJSONLine("deletion_stats", om.deletionResults); err != nil {
		return err
	}

	fmt.Printf("Generated deletion statistics: %s\n", path)

	// Generate deletion summary
	summaryPath, err := om.CreateOutputFile("deletion_summary", "txt")
	if err != nil {
		return err
	}

	summary := om.generateDeletionSummary()
	if err := om.WriteString("deletion_summary", summary); err != nil {
		return err
	}

	fmt.Printf("Generated deletion summary: %s\n", summaryPath)

	return nil
}

// generateDeletionSummary generates a human-readable deletion summary
func (om *OutputManager) generateDeletionSummary() string {
	var summary strings.Builder

	summary.WriteString("=== Deletion Operation Summary ===\n\n")
	summary.WriteString(fmt.Sprintf("Total Rows Processed: %d\n", om.deletionResults.TotalRows))
	summary.WriteString(fmt.Sprintf("Deleted Rows: %d\n", om.deletionResults.DeletedRows))
	summary.WriteString(fmt.Sprintf("Modified Rows: %d\n", om.deletionResults.ModifiedRows))
	summary.WriteString(fmt.Sprintf("Output Rows: %d\n", om.deletionResults.OutputRows))
	summary.WriteString(fmt.Sprintf("Errors: %d\n", om.deletionResults.ErrorCount))

	if len(om.deletionResults.MatchesByTarget) > 0 {
		summary.WriteString("\nMatches by Target:\n")
		for target, count := range om.deletionResults.MatchesByTarget {
			summary.WriteString(fmt.Sprintf("  %s: %d\n", target, count))
		}
	}

	duration := om.deletionResults.EndTime.Sub(om.deletionResults.StartTime)
	summary.WriteString(fmt.Sprintf("\nProcessing Time: %v\n", duration))

	return summary.String()
}

// ProcessDataWithOutput processes data sources and generates outputs
func (om *OutputManager) ProcessDataWithOutput(ctx context.Context, sources []source.InputSource, cfg *config.Config) error {
	// This would be the main processing loop that integrates with the analyser
	// For now, we'll create a placeholder

	fmt.Printf("Processing %d sources with output management...\n", len(sources))

	// Create output directory
	if err := os.MkdirAll(om.basePath, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Process each source
	for i, src := range sources {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fmt.Printf("Processing source %d/%d: %s\n", i+1, len(sources), src.Path())

		// Here you would integrate with the actual analyser
		// For now, we'll just create a placeholder output
		if err := om.processSource(src); err != nil {
			return fmt.Errorf("failed to process source %s: %w", src.Path(), err)
		}
	}

	// Generate all outputs
	if err := om.GenerateAllOutputs(cfg); err != nil {
		return fmt.Errorf("failed to generate outputs: %w", err)
	}

	return nil
}

// processSource processes a single source file
func (om *OutputManager) processSource(src source.InputSource) error {
	// Placeholder for source processing
	// In the actual implementation, this would integrate with the analyser
	fmt.Printf("  Processing file: %s (size: %d bytes)\n", src.Path(), src.Size())
	return nil
}

// Flush flushes all output writers
func (om *OutputManager) Flush() error {
	om.outputMutex.Lock()
	defer om.outputMutex.Unlock()

	var errors []error
	for name, writer := range om.outputWriters {
		if err := writer.Flush(); err != nil {
			errors = append(errors, fmt.Errorf("failed to flush %s: %w", name, err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("flush errors: %v", errors)
	}

	return nil
}

// Close closes all output files
func (om *OutputManager) Close() error {
	om.outputMutex.Lock()
	defer om.outputMutex.Unlock()

	var errors []error

	// Flush all writers first
	for name, writer := range om.outputWriters {
		if writer != nil {
			if err := writer.Flush(); err != nil {
				errors = append(errors, fmt.Errorf("failed to flush %s: %w", name, err))
			}
		}
	}

	// Close all files
	for name, file := range om.outputFiles {
		if file != nil {
			if err := file.Close(); err != nil {
				// Check if it's already closed
				if !strings.Contains(err.Error(), "file already closed") {
					errors = append(errors, fmt.Errorf("failed to close %s: %w", name, err))
				}
			}
		}
	}

	// Clear maps to prevent double-closing
	om.outputFiles = make(map[string]*os.File)
	om.outputWriters = make(map[string]*bufio.Writer)

	if len(errors) > 0 {
		return fmt.Errorf("close errors: %v", errors)
	}

	return nil
}

// WriteSearchResults writes search results in the specified format
func (om *OutputManager) WriteSearchResults(format config.OutputFormat) error {
	if om.searchResults == nil {
		return fmt.Errorf("search results not set")
	}

	// Create the file with exact filename (not timestamped)
	fullPath := filepath.Join(om.basePath, format.Filename)

	// Create directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %w", fullPath, err)
	}
	defer file.Close()

	switch format.Format {
	case "json":
		jsonData, err := json.Marshal(om.searchResults)
		if err != nil {
			return fmt.Errorf("failed to marshal search results: %w", err)
		}
		_, err = file.Write(jsonData)
		return err
	case "csv":
		_, err = file.WriteString(om.searchResultsCSV())
		return err
	case "txt":
		_, err = file.WriteString(om.searchResultsText())
		return err
	default:
		return fmt.Errorf("unsupported search results format: %s", format.Format)
	}
}

// WriteSchemaResults writes schema results in the specified format
func (om *OutputManager) WriteSchemaResults(format config.OutputFormat) error {
	if om.schemaAnalyzer == nil {
		return fmt.Errorf("schema analyzer not set")
	}

	schemaReport := om.schemaAnalyzer.GenerateReport()

	// Create the file with exact filename (not timestamped)
	fullPath := filepath.Join(om.basePath, format.Filename)

	// Create directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %w", fullPath, err)
	}
	defer file.Close()

	switch format.Format {
	case "json":
		jsonData, err := json.Marshal(schemaReport)
		if err != nil {
			return fmt.Errorf("failed to marshal schema report: %w", err)
		}
		_, err = file.Write(jsonData)
		return err
	case "csv":
		return om.writeSchemaCSVToFile(file, schemaReport)
	case "yaml":
		return om.writeSchemaYAMLToFile(file, schemaReport)
	default:
		return fmt.Errorf("unsupported schema results format: %s", format.Format)
	}
}

// WriteDeletionResults writes deletion results in the specified format
func (om *OutputManager) WriteDeletionResults(format config.OutputFormat) error {
	if om.deletionResults == nil {
		return fmt.Errorf("deletion results not set")
	}

	// Create the file with exact filename (not timestamped)
	fullPath := filepath.Join(om.basePath, format.Filename)

	// Create directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %w", fullPath, err)
	}
	defer file.Close()

	switch format.Format {
	case "json":
		jsonData, err := json.Marshal(om.deletionResults)
		if err != nil {
			return fmt.Errorf("failed to marshal deletion results: %w", err)
		}
		_, err = file.Write(jsonData)
		return err
	case "csv":
		_, err = file.WriteString(om.deletionResultsCSV())
		return err
	case "txt":
		_, err = file.WriteString(om.generateDeletionSummary())
		return err
	default:
		return fmt.Errorf("unsupported deletion results format: %s", format.Format)
	}
}

// ProcessSources processes data sources (placeholder for compatibility)
func (om *OutputManager) ProcessSources(ctx context.Context, sources []source.InputSource) error {
	return om.ProcessDataWithOutput(ctx, sources, &config.Config{})
}

// GetTimestamp returns the output manager's timestamp
func (om *OutputManager) GetTimestamp() string {
	return om.timestamp
}

// FlushAll flushes all output writers (alias for Flush)
func (om *OutputManager) FlushAll() error {
	return om.Flush()
}

// CreateDirectory creates a directory
func (om *OutputManager) CreateDirectory(path string) error {
	return os.MkdirAll(path, 0755)
}

// WriteMetadata writes metadata to a metadata file
func (om *OutputManager) WriteMetadata(metadata map[string]any) error {
	_, err := om.CreateOutputFile("metadata", "json")
	if err != nil {
		return err
	}
	err = om.WriteJSONLine("metadata", metadata)
	if err != nil {
		return err
	}
	// Flush the writer to ensure data is written
	return om.Flush()
}

// GenerateReport generates a comprehensive report
func (om *OutputManager) GenerateReport() *Report {
	return &Report{
		SearchResults: om.searchResults,
		SchemaReport:  om.currentSchemaReport(),
		DeletionStats: om.deletionResults,
		GeneratedAt:   time.Now(),
	}
}

func (om *OutputManager) searchResultsCSV() string {
	var b strings.Builder
	b.WriteString("target,path,matched_value,file_path,line_number\n")

	targets := make([]string, 0, len(om.searchResults.Results))
	for target := range om.searchResults.Results {
		targets = append(targets, target)
	}
	sort.Strings(targets)

	for _, target := range targets {
		for _, match := range om.searchResults.Results[target] {
			b.WriteString(fmt.Sprintf("%s,%s,%q,%s,%d\n",
				target,
				match.Path,
				match.MatchedValue,
				match.Location.FilePath,
				match.Location.LineNumber,
			))
		}
	}

	return b.String()
}

func (om *OutputManager) searchResultsText() string {
	var b strings.Builder
	b.WriteString("=== Search Results ===\n\n")
	b.WriteString(fmt.Sprintf("Total Matches: %d\n", om.searchResults.Summary.TotalMatches))
	b.WriteString(fmt.Sprintf("Processed Rows: %d\n\n", om.searchResults.Summary.ProcessedRows))

	targets := make([]string, 0, len(om.searchResults.Results))
	for target := range om.searchResults.Results {
		targets = append(targets, target)
	}
	sort.Strings(targets)

	for _, target := range targets {
		matches := om.searchResults.Results[target]
		b.WriteString(fmt.Sprintf("[%s] %d match(es)\n", target, len(matches)))
		for _, match := range matches {
			b.WriteString(fmt.Sprintf("- %s = %v (%s:%d)\n",
				match.Path,
				match.Value,
				match.Location.FilePath,
				match.Location.LineNumber,
			))
		}
		b.WriteString("\n")
	}

	return b.String()
}

func (om *OutputManager) deletionResultsCSV() string {
	var b strings.Builder
	b.WriteString("metric,value\n")
	b.WriteString(fmt.Sprintf("total_rows,%d\n", om.deletionResults.TotalRows))
	b.WriteString(fmt.Sprintf("processed_rows,%d\n", om.deletionResults.ProcessedRows))
	b.WriteString(fmt.Sprintf("deleted_rows,%d\n", om.deletionResults.DeletedRows))
	b.WriteString(fmt.Sprintf("modified_rows,%d\n", om.deletionResults.ModifiedRows))
	b.WriteString(fmt.Sprintf("output_rows,%d\n", om.deletionResults.OutputRows))
	b.WriteString(fmt.Sprintf("error_count,%d\n", om.deletionResults.ErrorCount))

	targets := make([]string, 0, len(om.deletionResults.MatchesByTarget))
	for target := range om.deletionResults.MatchesByTarget {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	for _, target := range targets {
		b.WriteString(fmt.Sprintf("matches_%s,%d\n", target, om.deletionResults.MatchesByTarget[target]))
	}

	return b.String()
}

// WriteAdvancedArtifacts writes advanced feature outputs derived from a completed report.
func WriteAdvancedArtifacts(basePath string, cfg *config.Config, rep *report.AnalysisReport) error {
	if rep == nil {
		return nil
	}

	om := NewOutputManager(basePath)
	defer om.Close()

	if searchResults, ok := rep.SearchResults.(*search.SearchResults); ok {
		om.SetSearchResults(searchResults)
	} else if searchResults, ok := rep.SearchResults.(search.SearchResults); ok {
		searchResultsCopy := searchResults
		om.SetSearchResults(&searchResultsCopy)
	}

	if schemaReport, ok := rep.SchemaReport.(*schema.SchemaReport); ok {
		om.SetSchemaReport(schemaReport)
	} else if schemaReport, ok := rep.SchemaReport.(schema.SchemaReport); ok {
		schemaReportCopy := schemaReport
		om.SetSchemaReport(&schemaReportCopy)
	}

	if deletionStats, ok := rep.DeletionStats.(*deletion.DeletionStats); ok {
		om.SetDeletionResults(deletionStats)
	} else if deletionStats, ok := rep.DeletionStats.(deletion.DeletionStats); ok {
		deletionStatsCopy := deletionStats
		om.SetDeletionResults(&deletionStatsCopy)
	}

	if om.searchResults != nil {
		if err := om.generateSearchResults(); err != nil {
			return err
		}
	}

	if om.currentSchemaReport() != nil {
		schemaCfg := config.SchemaDiscoveryConfig{OutputFormats: []string{"json"}}
		if cfg != nil && cfg.Advanced != nil && cfg.Advanced.SchemaDiscovery.Enabled {
			schemaCfg = cfg.Advanced.SchemaDiscovery
		}
		if err := om.generateSchemaReports(schemaCfg); err != nil {
			return err
		}
	}

	if om.deletionResults != nil {
		if err := om.generateDeletionReports(); err != nil {
			return err
		}
	}

	return om.Flush()
}

// Report represents a comprehensive analysis report
type Report struct {
	SearchResults *search.SearchResults   `json:"searchResults,omitempty"`
	SchemaReport  *schema.SchemaReport    `json:"schemaReport,omitempty"`
	DeletionStats *deletion.DeletionStats `json:"deletionStats,omitempty"`
	GeneratedAt   time.Time               `json:"generatedAt"`
}

// GetOutputPath returns the full path for a given output file name
func (om *OutputManager) GetOutputPath(name, extension string) string {
	filename := fmt.Sprintf("%s_%s.%s", name, om.timestamp, extension)
	return filepath.Join(om.basePath, filename)
}

// ListOutputFiles returns a list of all created output files
func (om *OutputManager) ListOutputFiles() []string {
	om.outputMutex.Lock()
	defer om.outputMutex.Unlock()

	var files []string
	for name := range om.outputFiles {
		files = append(files, name)
	}

	return files
}

// GetOutputStats returns statistics about the output files
func (om *OutputManager) GetOutputStats() map[string]os.FileInfo {
	om.outputMutex.Lock()
	defer om.outputMutex.Unlock()

	stats := make(map[string]os.FileInfo)
	for name, file := range om.outputFiles {
		if info, err := file.Stat(); err == nil {
			stats[name] = info
		}
	}

	return stats
}
