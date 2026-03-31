// Package output detects and writes analysis and ingest artefact formats.
package output

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// FileFormat represents the detected file format.
type FileFormat string

// FileFormat values describe the supported JSON-family encodings.
const (
	FormatJSON    FileFormat = "json"
	FormatNDJSON  FileFormat = "ndjson"
	FormatJSONL   FileFormat = "jsonl"
	FormatUnknown FileFormat = "unknown"
)

const (
	formatCSV            = "csv"
	extensionJSON        = ".json"
	extensionNDJSON      = ".ndjson"
	extensionJSONL       = ".jsonl"
	contentTypeJSON      = "application/json"
	contentTypeNDJSON    = "application/x-ndjson"
	contentTypeJSONLines = "application/jsonlines"
	contentTypeBinary    = "application/octet-stream"
)

// FormatDetector detects and validates JSON file formats.
type FormatDetector struct {
	samplesChecked int
	maxSamples     int
}

// NewFormatDetector creates a new format detector.
func NewFormatDetector(maxSamples int) *FormatDetector {
	if maxSamples <= 0 {
		maxSamples = 100 // Default sample size
	}
	return &FormatDetector{
		maxSamples: maxSamples,
	}
}

// DetectFormat detects the format of a file by analyzing its content.
func (fd *FormatDetector) DetectFormat(reader io.Reader) (FileFormat, error) {
	scanner := bufio.NewScanner(reader)

	var lines []string
	lineCount := 0

	// Read up to maxSamples lines
	for scanner.Scan() && lineCount < fd.maxSamples {
		line := strings.TrimSpace(scanner.Text())
		if line != "" { // Skip empty lines
			lines = append(lines, line)
			lineCount++
		}
	}

	if err := scanner.Err(); err != nil {
		return FormatUnknown, fmt.Errorf("error reading file: %w", err)
	}

	if len(lines) == 0 {
		return FormatUnknown, fmt.Errorf("file is empty or contains no valid content")
	}

	return fd.analyzeLines(lines)
}

// DetectFormatFromPath detects format from file path extension.
func (fd *FormatDetector) DetectFormatFromPath(path string) FileFormat {
	lowerPath := strings.ToLower(path)

	if strings.HasSuffix(lowerPath, ".json") {
		return FormatJSON
	} else if strings.HasSuffix(lowerPath, ".ndjson") {
		return FormatNDJSON
	} else if strings.HasSuffix(lowerPath, ".jsonl") {
		return FormatJSONL
	}

	return FormatUnknown
}

// analyzeLines analyzes the content lines to determine format.
func (fd *FormatDetector) analyzeLines(lines []string) (FileFormat, error) {
	if len(lines) == 0 {
		return FormatUnknown, fmt.Errorf("no content to analyze")
	}

	// Check if it's a single JSON document
	if len(lines) == 1 || fd.isSingleJSONDocument(lines) {
		return FormatJSON, nil
	}

	// Check if it's NDJSON/JSONL (multiple JSON objects, one per line)
	if fd.isNDJSON(lines) {
		return FormatNDJSON, nil
	}

	return FormatUnknown, fmt.Errorf("unable to determine file format")
}

// isSingleJSONDocument checks if the content represents a single JSON document.
func (fd *FormatDetector) isSingleJSONDocument(lines []string) bool {
	// Join all lines and try to parse as a single JSON document
	content := strings.Join(lines, "")

	// Try to parse as JSON
	var jsonData interface{}
	if err := json.Unmarshal([]byte(content), &jsonData); err != nil {
		return false
	}

	// Check if it's an array of objects (common JSON format)
	if array, ok := jsonData.([]interface{}); ok {
		// If it's an array, it's likely a single JSON document
		return len(array) > 0
	}

	// If it's an object, it's a single JSON document
	_, isObject := jsonData.(map[string]interface{})
	return isObject
}

// isNDJSON checks if the content represents NDJSON/JSONL format.
func (fd *FormatDetector) isNDJSON(lines []string) bool {
	validJSONLines := 0

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Each line should be a valid JSON object
		var jsonData interface{}
		if err := json.Unmarshal([]byte(line), &jsonData); err != nil {
			return false
		}

		// For NDJSON, each line should typically be an object
		if _, isObject := jsonData.(map[string]interface{}); isObject {
			validJSONLines++
		}
	}

	// If most lines are valid JSON objects, it's NDJSON
	return validJSONLines > 0 && validJSONLines >= len(lines)/2
}

// FormatInfo contains detailed information about the detected format.
type FormatInfo struct {
	Format      FileFormat
	LineCount   int
	IsValid     bool
	Extension   string
	ContentType string
	Description string
}

// GetFormatInfo returns detailed information about the detected format.
func (fd *FormatDetector) GetFormatInfo(reader io.Reader, path string) (*FormatInfo, error) {
	contentFormat, err := fd.DetectFormat(reader)
	if err != nil {
		return nil, err
	}

	pathFormat := fd.DetectFormatFromPath(path)

	// Use content-based detection as primary, path as fallback
	finalFormat := contentFormat
	if contentFormat == FormatUnknown && pathFormat != FormatUnknown {
		finalFormat = pathFormat
	}

	info := &FormatInfo{
		Format:    finalFormat,
		LineCount: fd.samplesChecked,
		IsValid:   finalFormat != FormatUnknown,
	}

	// Set format-specific details
	switch finalFormat {
	case FormatJSON:
		info.Extension = extensionJSON
		info.ContentType = contentTypeJSON
		info.Description = "Single JSON document"
	case FormatNDJSON:
		info.Extension = extensionNDJSON
		info.ContentType = contentTypeNDJSON
		info.Description = "Newline-delimited JSON"
	case FormatJSONL:
		info.Extension = extensionJSONL
		info.ContentType = contentTypeJSONLines
		info.Description = "JSON Lines format"
	case FormatUnknown:
		info.Extension = ""
		info.ContentType = contentTypeBinary
		info.Description = "Unknown format"
	}

	return info, nil
}

// ValidateFormat validates that a file matches the expected format.
func (fd *FormatDetector) ValidateFormat(reader io.Reader, expectedFormat FileFormat) error {
	detectedFormat, err := fd.DetectFormat(reader)
	if err != nil {
		return fmt.Errorf("failed to detect format: %w", err)
	}

	// NDJSON and JSONL are considered equivalent
	if (expectedFormat == FormatNDJSON && detectedFormat == FormatJSONL) ||
		(expectedFormat == FormatJSONL && detectedFormat == FormatNDJSON) {
		return nil
	}

	if detectedFormat != expectedFormat {
		return fmt.Errorf("format mismatch: expected %s, detected %s", expectedFormat, detectedFormat)
	}

	return nil
}

// GetRecommendedExtension returns the recommended file extension for a format.
func (fd *FormatDetector) GetRecommendedExtension(format FileFormat) string {
	switch format {
	case FormatJSON:
		return extensionJSON
	case FormatNDJSON:
		return extensionNDJSON
	case FormatJSONL:
		return extensionJSONL
	case FormatUnknown:
		return extensionJSON // Default fallback
	}

	return extensionJSON
}

// GetContentType returns the MIME content type for a format.
func (fd *FormatDetector) GetContentType(format FileFormat) string {
	switch format {
	case FormatJSON:
		return contentTypeJSON
	case FormatNDJSON:
		return contentTypeNDJSON
	case FormatJSONL:
		return contentTypeJSONLines
	case FormatUnknown:
		return contentTypeBinary
	}

	return contentTypeBinary
}

// FormatPreserver ensures format consistency across operations.
type FormatPreserver struct {
	detector *FormatDetector
}

// NewFormatPreserver creates a new format preserver.
func NewFormatPreserver() *FormatPreserver {
	return &FormatPreserver{
		detector: NewFormatDetector(100),
	}
}

// PreserveFormat ensures the output format matches the input format.
func (fp *FormatPreserver) PreserveFormat(inputPath, outputPath string, inputReader io.Reader) (*FormatInfo, error) {
	// Detect format from input
	formatInfo, err := fp.detector.GetFormatInfo(inputReader, inputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to detect input format: %w", err)
	}

	// Validate that output path has correct extension
	expectedExt := fp.detector.GetRecommendedExtension(formatInfo.Format)
	if !strings.HasSuffix(strings.ToLower(outputPath), expectedExt) {
		return nil, fmt.Errorf("output path %s should have extension %s for format %s",
			outputPath, expectedExt, formatInfo.Format)
	}

	return formatInfo, nil
}

// GenerateOutputPath generates an output path that preserves the input format.
func (fp *FormatPreserver) GenerateOutputPath(inputPath, outputDir, suffix string) (string, FileFormat, error) {
	// Detect format from path
	format := fp.detector.DetectFormatFromPath(inputPath)
	if format == FormatUnknown {
		return "", FormatUnknown, fmt.Errorf("unable to determine format from path: %s", inputPath)
	}

	// Extract base name without extension
	baseName := strings.TrimSuffix(inputPath, fp.detector.GetRecommendedExtension(format))
	if strings.Contains(baseName, "/") {
		parts := strings.Split(baseName, "/")
		baseName = parts[len(parts)-1]
	}

	// Generate output path
	extension := fp.detector.GetRecommendedExtension(format)
	outputPath := fmt.Sprintf("%s/%s%s%s", outputDir, baseName, suffix, extension)

	return outputPath, format, nil
}
