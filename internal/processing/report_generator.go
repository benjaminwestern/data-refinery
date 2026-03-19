// internal/processing/report_generator.go
package processing

import (
	"path/filepath"
	"strings"

	"github.com/benjaminwestern/data-refinery/internal/deletion"
	"github.com/benjaminwestern/data-refinery/internal/report"
	"github.com/benjaminwestern/data-refinery/internal/schema"
	"github.com/benjaminwestern/data-refinery/internal/search"
	"github.com/benjaminwestern/data-refinery/internal/source"
)

// ReportGenerator handles the generation of analysis reports
type ReportGenerator struct {
	searchEngine   *search.SearchEngine
	schemaAnalyzer *schema.SchemaAnalyzer
	deletionEngine *deletion.DeletionEngine
}

// NewReportGenerator creates a new report generator
func NewReportGenerator(searchEngine *search.SearchEngine, schemaAnalyzer *schema.SchemaAnalyzer, deletionEngine *deletion.DeletionEngine) *ReportGenerator {
	return &ReportGenerator{
		searchEngine:   searchEngine,
		schemaAnalyzer: schemaAnalyzer,
		deletionEngine: deletionEngine,
	}
}

// GenerateReport creates a comprehensive analysis report
func (rg *ReportGenerator) GenerateReport(
	sources []source.InputSource,
	rowProcessor *DefaultRowProcessor,
	totalRows int64,
	processedFiles int64,
	uniqueKey string,
	isValidation bool,
	wasCancelled bool,
) *report.AnalysisReport {
	rep := &report.AnalysisReport{
		DuplicateIDs:  make(map[string][]report.LocationInfo),
		DuplicateRows: make(map[string][]report.LocationInfo),
		Summary:       report.SummaryReport{},
	}

	// Get data from row processor
	idLocations := rowProcessor.GetIDLocations()
	rowHashes := rowProcessor.GetRowHashes()
	keysFoundPerFolder := rowProcessor.GetKeysFoundPerFolder()
	rowsProcessedPerFolder := rowProcessor.GetRowsProcessedPerFolder()
	filesProcessedPerFolder := rowProcessor.GetFilesProcessedPerFolder()

	// Process duplicate IDs
	for id, locations := range idLocations {
		if len(locations) > 1 {
			rep.DuplicateIDs[id] = locations
		}
	}

	// Process duplicate rows
	for hash, locations := range rowHashes {
		if len(locations) > 1 {
			rep.DuplicateRows[hash] = locations
		}
	}

	// Calculate total rows processed
	totalRowsProcessed := int64(0)
	for _, count := range rowsProcessedPerFolder {
		totalRowsProcessed += count
	}

	// Generate folder details
	folderDetails := make(map[string]report.FolderDetail)
	for _, src := range sources {
		dir := src.Dir()
		detail := folderDetails[dir]
		detail.TotalFiles++
		detail.TotalSizeBytes += src.Size()
		folderDetails[dir] = detail
	}

	for _, src := range sources {
		dir := src.Dir()
		detail := folderDetails[dir]
		detail.KeysFound = int(keysFoundPerFolder[dir])
		detail.RowsProcessed = int(rowsProcessedPerFolder[dir])
		detail.FilesProcessed = filesProcessedPerFolder[dir]
		if detail.FilesProcessed > 0 {
			detail.ProcessedSizeBytes = detail.TotalSizeBytes
		}
		folderDetails[dir] = detail
	}

	// Calculate total key occurrences
	totalKeyOccurrences := int64(0)
	uniqueKeysDuplicated := 0
	for _, locations := range idLocations {
		totalKeyOccurrences += int64(len(locations))
		if len(locations) > 1 {
			uniqueKeysDuplicated++
		}
	}

	// Calculate duplicate row instances
	duplicateRowInstances := 0
	duplicateIDsPerFolder := make(map[string]int)
	duplicateRowsPerFolder := make(map[string]int)
	for _, locations := range rowHashes {
		if len(locations) > 1 {
			duplicateRowInstances += len(locations)
			for _, location := range locations {
				duplicateRowsPerFolder[folderForPath(location.FilePath)]++
			}
		}
	}

	for _, locations := range idLocations {
		if len(locations) > 1 {
			for _, location := range locations {
				duplicateIDsPerFolder[folderForPath(location.FilePath)]++
			}
		}
	}

	// Set summary fields
	rep.Summary.IsValidationReport = isValidation
	rep.Summary.IsPartialReport = wasCancelled
	rep.Summary.FilesProcessed = int32(processedFiles)
	rep.Summary.TotalFiles = len(sources)
	rep.Summary.TotalRowsProcessed = totalRowsProcessed
	rep.Summary.UniqueKey = uniqueKey
	rep.Summary.TotalKeyOccurrences = int(totalKeyOccurrences)
	rep.Summary.UniqueKeysDuplicated = uniqueKeysDuplicated
	rep.Summary.DuplicateRowInstances = duplicateRowInstances
	rep.Summary.DuplicateIDsPerFolder = duplicateIDsPerFolder
	rep.Summary.DuplicateRowsPerFolder = duplicateRowsPerFolder
	rep.Summary.FolderDetails = folderDetails

	for _, src := range sources {
		rep.Summary.TotalDataSizeOverallBytes += src.Size()
	}
	rep.Summary.ProcessedDataSizeBytes = rep.Summary.TotalDataSizeOverallBytes
	rep.Summary.TotalDataSizeOverallHuman = report.HumanSize(rep.Summary.TotalDataSizeOverallBytes)
	rep.Summary.ProcessedDataSizeHuman = report.HumanSize(rep.Summary.ProcessedDataSizeBytes)

	// Calculate averages
	if processedFiles > 0 {
		rep.Summary.AverageRowsPerFile = float64(totalRowsProcessed) / float64(processedFiles)
	}
	if len(folderDetails) > 0 {
		rep.Summary.AverageFilesPerFolder = float64(processedFiles) / float64(len(folderDetails))
	}

	// Add advanced features to report
	if rg.searchEngine != nil {
		searchResults := rg.searchEngine.GetResults()
		rep.SearchResults = searchResults
	}

	if rg.schemaAnalyzer != nil {
		schemaReport := rg.schemaAnalyzer.GenerateReport()
		rep.SchemaReport = schemaReport
	}

	if rg.deletionEngine != nil {
		deletionStats := rg.deletionEngine.GetStats()
		rep.DeletionStats = deletionStats
	}

	return rep
}

func folderForPath(filePath string) string {
	if strings.HasPrefix(filePath, "gs://") {
		if idx := strings.LastIndex(filePath, "/"); idx > len("gs://") {
			return filePath[:idx]
		}
		return filePath
	}
	return filepath.Dir(filePath)
}

// GetSearchResults returns search results from the search engine
func (rg *ReportGenerator) GetSearchResults() *search.SearchResults {
	if rg.searchEngine == nil {
		return nil
	}
	results := rg.searchEngine.GetResults()
	return &results
}

// GetSchemaReport returns schema analysis results
func (rg *ReportGenerator) GetSchemaReport() *schema.SchemaReport {
	if rg.schemaAnalyzer == nil {
		return nil
	}
	return rg.schemaAnalyzer.GenerateReport()
}

// GetDeletionStats returns deletion statistics
func (rg *ReportGenerator) GetDeletionStats() *deletion.DeletionStats {
	if rg.deletionEngine == nil {
		return nil
	}
	stats := rg.deletionEngine.GetStats()
	return &stats
}
