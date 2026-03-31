// Package schema discovers field structure and emits schema reports.
package schema

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/report"
	"github.com/benjaminwestern/data-refinery/internal/safety"
	"gopkg.in/yaml.v3"
)

const (
	fieldTypeNull   = "null"
	fieldTypeMixed  = "mixed"
	fieldTypeObject = "object"
)

// Analyzer handles schema discovery and analysis.
type Analyzer struct {
	config        config.SchemaDiscoveryConfig
	globalSchema  *Schema
	folderSchemas map[string]*Schema
	sampledRows   int64
	totalRows     int64
	mutex         sync.RWMutex
}

// Schema represents the discovered schema structure.
type Schema struct {
	Fields       map[string]*FieldSchema `json:"fields"`
	TotalSamples int64                   `json:"totalSamples"`
	mutex        sync.RWMutex
}

// FieldSchema contains metadata about a field.
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

// Report contains the complete schema analysis results.
type Report struct {
	GlobalSchema  *Schema            `json:"globalSchema"`
	FolderSchemas map[string]*Schema `json:"folderSchemas,omitempty"`
	TotalRows     int64              `json:"totalRows"`
	SampledRows   int64              `json:"sampledRows"`
	SampleRate    float64            `json:"sampleRate"`
}

// NewSchemaAnalyzer creates a new schema analyzer.
func NewSchemaAnalyzer(config config.SchemaDiscoveryConfig) *Analyzer {
	return &Analyzer{
		config:        config,
		globalSchema:  newSchema(),
		folderSchemas: make(map[string]*Schema),
	}
}

// newSchema creates a new schema instance.
func newSchema() *Schema {
	return &Schema{
		Fields: make(map[string]*FieldSchema),
	}
}

// newFieldSchema creates a new field schema instance.
func newFieldSchema(path string) *FieldSchema {
	return &FieldSchema{
		Path:         path,
		Examples:     make([]any, 0, 5),
		UniqueValues: make(map[string]bool),
		SubFields:    make(map[string]*FieldSchema),
	}
}

// AnalyzeRow processes a single row for schema discovery.
func (sa *Analyzer) AnalyzeRow(data report.JSONData, filePath string) {
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

// shouldSample determines if this row should be sampled.
func (sa *Analyzer) shouldSample() bool {
	if sa.sampledRows >= int64(sa.config.MaxSamples) {
		return false
	}
	return safety.RandomFloat64() < sa.config.SamplePercent
}

// ensureFolderSchema gets or creates a schema for a folder.
func (sa *Analyzer) ensureFolderSchema(folder string) *Schema {
	sa.mutex.Lock()
	defer sa.mutex.Unlock()

	if schema, exists := sa.folderSchemas[folder]; exists {
		return schema
	}

	schema := newSchema()
	sa.folderSchemas[folder] = schema
	return schema
}

// analyzeFields recursively analyzes fields in the data.
func (sa *Analyzer) analyzeFields(data report.JSONData, schema *Schema, prefix string, depth int) {
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

// buildPath constructs the full path for nested fields.
func (sa *Analyzer) buildPath(prefix, key string) string {
	if prefix == "" {
		return key
	}
	return fmt.Sprintf("%s.%s", prefix, key)
}

// ensureFieldSchema gets or creates a field schema.
func (sa *Analyzer) ensureFieldSchema(schema *Schema, path string) *FieldSchema {
	schema.mutex.Lock()
	defer schema.mutex.Unlock()

	if field, exists := schema.Fields[path]; exists {
		return field
	}

	field := newFieldSchema(path)
	schema.Fields[path] = field
	return field
}

// updateFieldSchema updates a field schema with new data.
func (sa *Analyzer) updateFieldSchema(field *FieldSchema, value any, depth int) {
	field.mutex.Lock()
	defer field.mutex.Unlock()

	field.Occurrences++

	// Handle null values
	if value == nil {
		field.IsNullable = true
		if field.Type == "" {
			field.Type = fieldTypeNull
		} else if field.Type != fieldTypeNull && field.Type != fieldTypeMixed {
			field.Type = fieldTypeMixed
		}
		return
	}

	// Determine and update type
	valueType := sa.getValueType(value)
	if field.Type == "" {
		field.Type = valueType
	} else if field.Type != valueType && field.Type != fieldTypeMixed {
		field.Type = fieldTypeMixed
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

	sa.updateStringLengthStats(field, valueType, value)
	sa.updateArraySchema(field, valueType, value, depth)

	// Handle objects
	if valueType == "object" {
		if objData, ok := value.(map[string]any); ok && depth < sa.config.MaxDepth {
			sa.analyzeObjectFields(objData, field, field.Path, depth+1)
		}
	}
}

func (sa *Analyzer) updateStringLengthStats(field *FieldSchema, valueType string, value any) {
	if valueType != "string" {
		return
	}

	str, ok := value.(string)
	if !ok {
		return
	}

	length := len(str)
	if field.MinLength == 0 || length < field.MinLength {
		field.MinLength = length
	}
	if length > field.MaxLength {
		field.MaxLength = length
	}
}

func (sa *Analyzer) updateArraySchema(field *FieldSchema, valueType string, value any, depth int) {
	if valueType != "array" {
		return
	}

	arr, ok := value.([]any)
	if !ok || len(arr) == 0 {
		return
	}

	elementType := sa.getValueType(arr[0])
	if field.ArrayElementType == "" {
		field.ArrayElementType = elementType
	} else if field.ArrayElementType != elementType {
		field.ArrayElementType = fieldTypeMixed
	}

	if elementType != fieldTypeObject || depth >= sa.config.MaxDepth {
		return
	}

	for i, elem := range arr {
		objData, ok := elem.(map[string]any)
		if !ok {
			continue
		}
		arrayPath := fmt.Sprintf("%s[%d]", field.Path, i)
		sa.analyzeObjectFields(objData, field, arrayPath, depth+1)
	}
}

// analyzeObjectFields analyzes fields within an object.
func (sa *Analyzer) analyzeObjectFields(data map[string]any, parentField *FieldSchema, prefix string, depth int) {
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

// getValueType determines the type of a value.
func (sa *Analyzer) getValueType(value any) string {
	if value == nil {
		return "null"
	}

	kind := reflect.TypeOf(value).Kind()

	if kind == reflect.String {
		return "string"
	}

	if kind == reflect.Int || kind == reflect.Int8 || kind == reflect.Int16 || kind == reflect.Int32 || kind == reflect.Int64 ||
		kind == reflect.Uint || kind == reflect.Uint8 || kind == reflect.Uint16 || kind == reflect.Uint32 || kind == reflect.Uint64 {
		return "integer"
	}

	if kind == reflect.Float32 || kind == reflect.Float64 {
		return "number"
	}

	if kind == reflect.Bool {
		return "boolean"
	}

	if kind == reflect.Slice || kind == reflect.Array {
		return "array"
	}

	if kind == reflect.Map {
		return "object"
	}

	return "unknown"
}

// GenerateReport creates a schema report.
func (sa *Analyzer) GenerateReport() *Report {
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

	return &Report{
		GlobalSchema:  sa.globalSchema,
		FolderSchemas: sa.folderSchemas,
		TotalRows:     sa.totalRows,
		SampledRows:   sa.sampledRows,
		SampleRate:    sampleRate,
	}
}

// calculatePercentages calculates occurrence percentages for all fields.
func (sa *Analyzer) calculatePercentages(schema *Schema) {
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

// calculateSubFieldPercentages calculates percentages for nested fields.
func (sa *Analyzer) calculateSubFieldPercentages(field *FieldSchema, total float64) {
	field.mutex.Lock()
	defer field.mutex.Unlock()

	for _, subField := range field.SubFields {
		subField.mutex.Lock()
		subField.Percentage = (float64(subField.Occurrences) / total) * 100
		subField.mutex.Unlock()

		sa.calculateSubFieldPercentages(subField, total)
	}
}

// SaveReport saves the schema report in specified formats.
func (sa *Analyzer) SaveReport(baseFilename string, formats []string) error {
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

// saveJSONReport saves the report as JSON.
func (sa *Analyzer) saveJSONReport(report *Report, filename string) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	if err := os.WriteFile(filename, data, 0o600); err != nil {
		return fmt.Errorf("write JSON schema report: %w", err)
	}

	return nil
}

// saveCSVReport saves the report as CSV.
func (sa *Analyzer) saveCSVReport(report *Report, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create CSV file: %w", err)
	}
	defer safety.Close(file, filename)

	writer := csv.NewWriter(file)

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

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush CSV schema report: %w", err)
	}

	return nil
}

// writeSchemaToCSV writes a schema to CSV format.
func (sa *Analyzer) writeSchemaToCSV(writer *csv.Writer, schema *Schema, prefix string) error {
	// Sort fields by path for consistent output
	paths := make([]string, 0, len(schema.Fields))
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

// writeFieldToCSV writes a field to CSV format.
func (sa *Analyzer) writeFieldToCSV(writer *csv.Writer, field *FieldSchema, prefix string) error {
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

	if err := writer.Write(record); err != nil {
		return fmt.Errorf("write schema CSV record: %w", err)
	}

	return nil
}

// saveYAMLReport saves the report as YAML.
func (sa *Analyzer) saveYAMLReport(report *Report, filename string) error {
	data, err := yaml.Marshal(report)
	if err != nil {
		return fmt.Errorf("failed to marshal YAML: %w", err)
	}

	if err := os.WriteFile(filename, data, 0o600); err != nil {
		return fmt.Errorf("write YAML schema report: %w", err)
	}

	return nil
}
