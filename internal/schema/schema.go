package schema

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/report"
	"gopkg.in/yaml.v3"
)

// SchemaAnalyzer handles schema discovery and analysis
type SchemaAnalyzer struct {
	config        config.SchemaDiscoveryConfig
	globalSchema  *Schema
	folderSchemas map[string]*Schema
	sampledRows   int64
	totalRows     int64
	mutex         sync.RWMutex
}

// Schema represents the discovered schema structure
type Schema struct {
	Fields       map[string]*FieldSchema `json:"fields"`
	TotalSamples int64                   `json:"totalSamples"`
	mutex        sync.RWMutex
}

// FieldSchema contains metadata about a field
type FieldSchema struct {
	Path             string                  `json:"path"`
	Type             string                  `json:"type"`
	Occurrences      int64                   `json:"occurrences"`
	Percentage       float64                 `json:"percentage"`
	IsNullable       bool                    `json:"isNullable"`
	ArrayElementType string                  `json:"arrayElementType,omitempty"`
	SubFields        map[string]*FieldSchema `json:"subFields,omitempty"`
	Examples         []any                   `json:"examples,omitempty"`
	MinLength        int                     `json:"minLength,omitempty"`
	MaxLength        int                     `json:"maxLength,omitempty"`
	UniqueValues     map[string]bool         `json:"-"`
	UniqueCount      int64                   `json:"uniqueCount"`
	mutex            sync.RWMutex
}

// SchemaReport contains the complete schema analysis results
type SchemaReport struct {
	GlobalSchema  *Schema            `json:"globalSchema"`
	FolderSchemas map[string]*Schema `json:"folderSchemas,omitempty"`
	TotalRows     int64              `json:"totalRows"`
	SampledRows   int64              `json:"sampledRows"`
	SampleRate    float64            `json:"sampleRate"`
}

// NewSchemaAnalyzer creates a new schema analyzer
func NewSchemaAnalyzer(config config.SchemaDiscoveryConfig) *SchemaAnalyzer {
	return &SchemaAnalyzer{
		config:        config,
		globalSchema:  newSchema(),
		folderSchemas: make(map[string]*Schema),
	}
}

// newSchema creates a new schema instance
func newSchema() *Schema {
	return &Schema{
		Fields: make(map[string]*FieldSchema),
	}
}

// newFieldSchema creates a new field schema instance
func newFieldSchema(path string) *FieldSchema {
	return &FieldSchema{
		Path:         path,
		Examples:     make([]any, 0, 5),
		UniqueValues: make(map[string]bool),
		SubFields:    make(map[string]*FieldSchema),
	}
}

// AnalyzeRow processes a single row for schema discovery
func (sa *SchemaAnalyzer) AnalyzeRow(data report.JSONData, filePath string) {
	sa.mutex.Lock()
	sa.totalRows++
	shouldSample := sa.shouldSample()
	sa.mutex.Unlock()

	if !shouldSample {
		return
	}

	sa.mutex.Lock()
	sa.sampledRows++
	sa.mutex.Unlock()

	folder := schemaFolderFromPath(filePath)

	// Analyze for global schema
	sa.analyzeFields(data, sa.globalSchema, "", 0)

	// Analyze for folder-specific schema
	if sa.config.GroupByFolder {
		folderSchema := sa.ensureFolderSchema(folder)
		sa.analyzeFields(data, folderSchema, "", 0)
	}
}

func schemaFolderFromPath(filePath string) string {
	if strings.HasPrefix(filePath, "gs://") {
		if idx := strings.LastIndex(filePath, "/"); idx > len("gs://") {
			return filePath[:idx]
		}
		return filePath
	}
	return filepath.Dir(filePath)
}

// shouldSample determines if this row should be sampled
func (sa *SchemaAnalyzer) shouldSample() bool {
	if sa.sampledRows >= int64(sa.config.MaxSamples) {
		return false
	}
	return rand.Float64() < sa.config.SamplePercent
}

// ensureFolderSchema gets or creates a schema for a folder
func (sa *SchemaAnalyzer) ensureFolderSchema(folder string) *Schema {
	sa.mutex.Lock()
	defer sa.mutex.Unlock()

	if schema, exists := sa.folderSchemas[folder]; exists {
		return schema
	}

	schema := newSchema()
	sa.folderSchemas[folder] = schema
	return schema
}

// analyzeFields recursively analyzes fields in the data
func (sa *SchemaAnalyzer) analyzeFields(data report.JSONData, schema *Schema, prefix string, depth int) {
	if depth > sa.config.MaxDepth {
		return
	}

	schema.mutex.Lock()
	schema.TotalSamples++
	schema.mutex.Unlock()

	for key, value := range data {
		fullPath := sa.buildPath(prefix, key)
		fieldSchema := sa.ensureFieldSchema(schema, fullPath)
		sa.updateFieldSchema(fieldSchema, value, depth)
	}
}

// buildPath constructs the full path for nested fields
func (sa *SchemaAnalyzer) buildPath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return fmt.Sprintf("%s.%s", prefix, key)
}

// ensureFieldSchema gets or creates a field schema
func (sa *SchemaAnalyzer) ensureFieldSchema(schema *Schema, path string) *FieldSchema {
	schema.mutex.Lock()
	defer schema.mutex.Unlock()

	if field, exists := schema.Fields[path]; exists {
		return field
	}

	field := newFieldSchema(path)
	schema.Fields[path] = field
	return field
}

// updateFieldSchema updates a field schema with new data
func (sa *SchemaAnalyzer) updateFieldSchema(field *FieldSchema, value any, depth int) {
	field.mutex.Lock()
	defer field.mutex.Unlock()

	field.Occurrences++

	// Handle null values
	if value == nil {
		field.IsNullable = true
		if field.Type == "" {
			field.Type = "null"
		} else if field.Type != "null" && field.Type != "mixed" {
			field.Type = "mixed"
		}
		return
	}

	// Determine and update type
	valueType := sa.getValueType(value)
	if field.Type == "" {
		field.Type = valueType
	} else if field.Type != valueType && field.Type != "mixed" {
		field.Type = "mixed"
	}

	// Add examples (limit to 5)
	if len(field.Examples) < 5 {
		field.Examples = append(field.Examples, value)
	}

	// Track unique values (limit to prevent memory issues)
	if len(field.UniqueValues) < 10000 {
		valueStr := fmt.Sprintf("%v", value)
		if !field.UniqueValues[valueStr] {
			field.UniqueValues[valueStr] = true
			field.UniqueCount++
		}
	}

	// Update string length stats
	if valueType == "string" {
		if str, ok := value.(string); ok {
			length := len(str)
			if field.MinLength == 0 || length < field.MinLength {
				field.MinLength = length
			}
			if length > field.MaxLength {
				field.MaxLength = length
			}
		}
	}

	// Handle arrays
	if valueType == "array" {
		if arr, ok := value.([]any); ok && len(arr) > 0 {
			elementType := sa.getValueType(arr[0])
			if field.ArrayElementType == "" {
				field.ArrayElementType = elementType
			} else if field.ArrayElementType != elementType {
				field.ArrayElementType = "mixed"
			}

			// Analyze array elements if they're objects
			if elementType == "object" && depth < sa.config.MaxDepth {
				for i, elem := range arr {
					if objData, ok := elem.(map[string]any); ok {
						arrayPath := fmt.Sprintf("%s[%d]", field.Path, i)
						sa.analyzeObjectFields(objData, field, arrayPath, depth+1)
					}
				}
			}
		}
	}

	// Handle objects
	if valueType == "object" {
		if objData, ok := value.(map[string]any); ok && depth < sa.config.MaxDepth {
			sa.analyzeObjectFields(objData, field, field.Path, depth+1)
		}
	}
}

// analyzeObjectFields analyzes fields within an object
func (sa *SchemaAnalyzer) analyzeObjectFields(data map[string]any, parentField *FieldSchema, prefix string, depth int) {
	for key, value := range data {
		fullPath := fmt.Sprintf("%s.%s", prefix, key)

		if parentField.SubFields == nil {
			parentField.SubFields = make(map[string]*FieldSchema)
		}

		if _, exists := parentField.SubFields[key]; !exists {
			parentField.SubFields[key] = newFieldSchema(fullPath)
		}

		sa.updateFieldSchema(parentField.SubFields[key], value, depth)
	}
}

// getValueType determines the type of a value
func (sa *SchemaAnalyzer) getValueType(value any) string {
	if value == nil {
		return "null"
	}

	switch reflect.TypeOf(value).Kind() {
	case reflect.String:
		return "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer"
	case reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Bool:
		return "boolean"
	case reflect.Slice, reflect.Array:
		return "array"
	case reflect.Map:
		return "object"
	default:
		return "unknown"
	}
}

// GenerateReport creates a schema report
func (sa *SchemaAnalyzer) GenerateReport() *SchemaReport {
	sa.mutex.RLock()
	defer sa.mutex.RUnlock()

	// Calculate percentages for global schema
	sa.calculatePercentages(sa.globalSchema)

	// Calculate percentages for folder schemas
	for _, schema := range sa.folderSchemas {
		sa.calculatePercentages(schema)
	}

	sampleRate := 0.0
	if sa.totalRows > 0 {
		sampleRate = float64(sa.sampledRows) / float64(sa.totalRows)
	}

	return &SchemaReport{
		GlobalSchema:  sa.globalSchema,
		FolderSchemas: sa.folderSchemas,
		TotalRows:     sa.totalRows,
		SampledRows:   sa.sampledRows,
		SampleRate:    sampleRate,
	}
}

// calculatePercentages calculates occurrence percentages for all fields
func (sa *SchemaAnalyzer) calculatePercentages(schema *Schema) {
	schema.mutex.Lock()
	defer schema.mutex.Unlock()

	total := float64(schema.TotalSamples)
	if total == 0 {
		return
	}

	for _, field := range schema.Fields {
		field.mutex.Lock()
		field.Percentage = (float64(field.Occurrences) / total) * 100
		field.mutex.Unlock()

		sa.calculateSubFieldPercentages(field, total)
	}
}

// calculateSubFieldPercentages calculates percentages for nested fields
func (sa *SchemaAnalyzer) calculateSubFieldPercentages(field *FieldSchema, total float64) {
	field.mutex.Lock()
	defer field.mutex.Unlock()

	for _, subField := range field.SubFields {
		subField.mutex.Lock()
		subField.Percentage = (float64(subField.Occurrences) / total) * 100
		subField.mutex.Unlock()

		sa.calculateSubFieldPercentages(subField, total)
	}
}

// SaveReport saves the schema report in specified formats
func (sa *SchemaAnalyzer) SaveReport(baseFilename string, formats []string) error {
	report := sa.GenerateReport()

	for _, format := range formats {
		switch strings.ToLower(format) {
		case "json":
			if err := sa.saveJSONReport(report, baseFilename+".json"); err != nil {
				return err
			}
		case "csv":
			if err := sa.saveCSVReport(report, baseFilename+".csv"); err != nil {
				return err
			}
		case "yaml":
			if err := sa.saveYAMLReport(report, baseFilename+".yaml"); err != nil {
				return err
			}
		}
	}

	return nil
}

// saveJSONReport saves the report as JSON
func (sa *SchemaAnalyzer) saveJSONReport(report *SchemaReport, filename string) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	return os.WriteFile(filename, data, 0o644)
}

// saveCSVReport saves the report as CSV
func (sa *SchemaAnalyzer) saveCSVReport(report *SchemaReport, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{"Path", "Type", "Occurrences", "Percentage", "IsNullable", "UniqueCount", "MinLength", "MaxLength", "Examples"}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}

	// Write global schema
	if err := sa.writeSchemaToCSV(writer, report.GlobalSchema, "global"); err != nil {
		return err
	}

	// Write folder schemas
	for folder, schema := range report.FolderSchemas {
		if err := sa.writeSchemaToCSV(writer, schema, folder); err != nil {
			return err
		}
	}

	return nil
}

// writeSchemaToCSV writes a schema to CSV format
func (sa *SchemaAnalyzer) writeSchemaToCSV(writer *csv.Writer, schema *Schema, prefix string) error {
	// Sort fields by path for consistent output
	var paths []string
	for path := range schema.Fields {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	for _, path := range paths {
		field := schema.Fields[path]
		if err := sa.writeFieldToCSV(writer, field, prefix); err != nil {
			return err
		}
	}

	return nil
}

// writeFieldToCSV writes a field to CSV format
func (sa *SchemaAnalyzer) writeFieldToCSV(writer *csv.Writer, field *FieldSchema, prefix string) error {
	examples := make([]string, len(field.Examples))
	for i, ex := range field.Examples {
		examples[i] = fmt.Sprintf("%v", ex)
	}

	record := []string{
		fmt.Sprintf("%s:%s", prefix, field.Path),
		field.Type,
		strconv.FormatInt(field.Occurrences, 10),
		fmt.Sprintf("%.2f", field.Percentage),
		strconv.FormatBool(field.IsNullable),
		strconv.FormatInt(field.UniqueCount, 10),
		strconv.Itoa(field.MinLength),
		strconv.Itoa(field.MaxLength),
		strings.Join(examples, "; "),
	}

	return writer.Write(record)
}

// saveYAMLReport saves the report as YAML
func (sa *SchemaAnalyzer) saveYAMLReport(report *SchemaReport, filename string) error {
	data, err := yaml.Marshal(report)
	if err != nil {
		return fmt.Errorf("failed to marshal YAML: %w", err)
	}

	return os.WriteFile(filename, data, 0o644)
}
