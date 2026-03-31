package ingest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const mappingExtensionJSON = ".json"

var supportedStandardFields = map[string]struct{}{
	"Id":                 {},
	"FirstName":          {},
	"LastName":           {},
	"Email":              {},
	"Mobile":             {},
	"PostCode":           {},
	"DataSource":         {},
	"SourceCreatedDate":  {},
	"SourceModifiedDate": {},
}

var defaultNullValues = []string{"", "null", "NULL", "n/a", "N/A"}

// MappingSet contains the normalized mapping rules used by the ingest
// workflow. It accepts both the legacy py-file-ingestion format and the newer
// Data Refinery structured format.
type MappingSet struct {
	Defaults MappingDefaults `json:"defaults" yaml:"defaults"`
	Files    []FileMapping   `json:"files" yaml:"files"`
}

// MappingDefaults provides global mapping behavior that individual file
// mappings can override.
type MappingDefaults struct {
	IncludeUnmappedAsAttributes bool     `json:"includeUnmappedAsAttributes" yaml:"includeUnmappedAsAttributes"`
	NullValues                  []string `json:"nullValues" yaml:"nullValues"`
	TrimWhitespace              *bool    `json:"trimWhitespace" yaml:"trimWhitespace"`
}

// FileMatch identifies which source files a mapping applies to.
type FileMatch struct {
	Name   string `json:"name" yaml:"name"`
	Glob   string `json:"glob" yaml:"glob"`
	Format string `json:"format" yaml:"format"`
}

// FileMapping describes how a matched input file is normalized.
type FileMapping struct {
	Match                       FileMatch         `json:"match" yaml:"match"`
	Columns                     map[string]string `json:"columns" yaml:"columns"`
	Attributes                  map[string]string `json:"attributes" yaml:"attributes"`
	Defaults                    map[string]string `json:"defaults" yaml:"defaults"`
	DataSource                  string            `json:"dataSource" yaml:"dataSource"`
	LegacyDataSource            string            `json:"data_source" yaml:"data_source"`
	Sheet                       string            `json:"sheet" yaml:"sheet"`
	SourceCreatedDateField      string            `json:"sourceCreatedDateField" yaml:"sourceCreatedDateField"`
	SourceModifiedDateField     string            `json:"sourceModifiedDateField" yaml:"sourceModifiedDateField"`
	IncludeUnmappedAsAttributes *bool             `json:"includeUnmappedAsAttributes" yaml:"includeUnmappedAsAttributes"`
	NullValues                  []string          `json:"nullValues" yaml:"nullValues"`
	TrimWhitespace              *bool             `json:"trimWhitespace" yaml:"trimWhitespace"`
}

// ResolvedFileMapping is the effective mapping configuration after defaults are
// applied.
type ResolvedFileMapping struct {
	FileMapping
	EffectiveDataSource      string
	EffectiveNullValues      []string
	EffectiveTrimWhitespace  bool
	EffectiveIncludeUnmapped bool
}

type legacyFileMapping struct {
	Columns                 map[string]string `json:"columns" yaml:"columns"`
	Attributes              map[string]string `json:"attributes" yaml:"attributes"`
	DataSource              string            `json:"data_source" yaml:"data_source"`
	Sheet                   string            `json:"sheet" yaml:"sheet"`
	SourceCreatedDateField  string            `json:"sourceCreatedDateField" yaml:"sourceCreatedDateField"`
	SourceModifiedDateField string            `json:"sourceModifiedDateField" yaml:"sourceModifiedDateField"`
}

// LoadMappingSet reads a YAML or JSON mapping file and normalizes it into the
// internal mapping representation.
func LoadMappingSet(path string) (*MappingSet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read mapping file: %w", err)
	}

	var envelope map[string]any
	if err := unmarshalMappingData(data, path, &envelope); err != nil {
		return nil, err
	}

	var mappings MappingSet
	if isStructuredMapping(envelope) {
		if err := unmarshalMappingData(data, path, &mappings); err != nil {
			return nil, err
		}
	} else {
		legacyMappings := make(map[string]legacyFileMapping)
		if err := unmarshalMappingData(data, path, &legacyMappings); err != nil {
			return nil, err
		}

		fileNames := make([]string, 0, len(legacyMappings))
		for fileName := range legacyMappings {
			fileNames = append(fileNames, fileName)
		}
		sort.Strings(fileNames)

		mappings.Files = make([]FileMapping, 0, len(fileNames))
		for _, fileName := range fileNames {
			legacy := legacyMappings[fileName]
			mappings.Files = append(mappings.Files, FileMapping{
				Match:                   FileMatch{Name: fileName},
				Columns:                 cloneStringMap(legacy.Columns),
				Attributes:              cloneStringMap(legacy.Attributes),
				LegacyDataSource:        legacy.DataSource,
				Sheet:                   legacy.Sheet,
				SourceCreatedDateField:  legacy.SourceCreatedDateField,
				SourceModifiedDateField: legacy.SourceModifiedDateField,
			})
		}
	}

	mappings.applyDefaults()
	if err := mappings.Validate(); err != nil {
		return nil, err
	}

	return &mappings, nil
}

// Resolve selects the first matching file rule for the supplied source path and
// format, then applies the global defaults.
func (ms *MappingSet) Resolve(sourcePath, format string) *ResolvedFileMapping {
	if ms == nil {
		return nil
	}

	fileName := filepath.Base(sourcePath)
	for _, mapping := range ms.Files {
		if !mapping.matches(fileName, format) {
			continue
		}

		trimWhitespace := true
		if ms.Defaults.TrimWhitespace != nil {
			trimWhitespace = *ms.Defaults.TrimWhitespace
		}
		if mapping.TrimWhitespace != nil {
			trimWhitespace = *mapping.TrimWhitespace
		}

		includeUnmapped := ms.Defaults.IncludeUnmappedAsAttributes
		if mapping.IncludeUnmappedAsAttributes != nil {
			includeUnmapped = *mapping.IncludeUnmappedAsAttributes
		}

		nullValues := cloneStrings(ms.Defaults.NullValues)
		if len(mapping.NullValues) > 0 {
			nullValues = cloneStrings(mapping.NullValues)
		}

		return &ResolvedFileMapping{
			FileMapping:              mapping,
			EffectiveDataSource:      mapping.effectiveDataSource(),
			EffectiveNullValues:      nullValues,
			EffectiveTrimWhitespace:  trimWhitespace,
			EffectiveIncludeUnmapped: includeUnmapped,
		}
	}

	return nil
}

// Validate checks whether the mapping set is internally consistent.
func (ms *MappingSet) Validate() error {
	if ms == nil {
		return fmt.Errorf("mapping set cannot be nil")
	}
	if len(ms.Files) == 0 {
		return fmt.Errorf("mapping file must define at least one file mapping")
	}

	for index, mapping := range ms.Files {
		if err := mapping.Validate(); err != nil {
			return fmt.Errorf("file mapping %d: %w", index+1, err)
		}
	}

	return nil
}

// Validate checks whether the file mapping targets valid schema fields.
func (fm *FileMapping) Validate() error {
	if fm == nil {
		return fmt.Errorf("file mapping cannot be nil")
	}
	if strings.TrimSpace(fm.Match.Name) == "" && strings.TrimSpace(fm.Match.Glob) == "" {
		return fmt.Errorf("match.name or match.glob is required")
	}

	for sourceField, targetField := range fm.Columns {
		if strings.TrimSpace(sourceField) == "" {
			return fmt.Errorf("column mappings cannot contain an empty source field")
		}
		if !isSupportedStandardField(targetField) {
			return fmt.Errorf("unsupported target field %q", targetField)
		}
	}

	for targetField := range fm.Defaults {
		if !isSupportedStandardField(targetField) {
			return fmt.Errorf("unsupported default field %q", targetField)
		}
	}

	if fm.Match.Format != "" && normalizeInputFormat(fm.Match.Format) == "" {
		return fmt.Errorf("unsupported match format %q", fm.Match.Format)
	}

	return nil
}

func (ms *MappingSet) applyDefaults() {
	if len(ms.Defaults.NullValues) == 0 {
		ms.Defaults.NullValues = cloneStrings(defaultNullValues)
	}
	if ms.Defaults.TrimWhitespace == nil {
		trimWhitespace := true
		ms.Defaults.TrimWhitespace = &trimWhitespace
	}
}

func (fm FileMapping) matches(fileName, format string) bool {
	if fm.Match.Format != "" && normalizeInputFormat(fm.Match.Format) != format {
		return false
	}

	if fm.Match.Name != "" {
		return strings.EqualFold(fm.Match.Name, fileName)
	}

	matched, err := filepath.Match(fm.Match.Glob, fileName)
	return err == nil && matched
}

func (fm FileMapping) effectiveDataSource() string {
	if strings.TrimSpace(fm.DataSource) != "" {
		return fm.DataSource
	}
	return fm.LegacyDataSource
}

func unmarshalMappingData(data []byte, path string, target any) error {
	switch strings.ToLower(filepath.Ext(path)) {
	case mappingExtensionJSON:
		if err := json.Unmarshal(data, target); err != nil {
			return fmt.Errorf("parse JSON mapping file: %w", err)
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, target); err != nil {
			return fmt.Errorf("parse YAML mapping file: %w", err)
		}
	default:
		if err := json.Unmarshal(data, target); err == nil {
			return nil
		}
		if err := yaml.Unmarshal(data, target); err != nil {
			return fmt.Errorf("mapping file must be JSON or YAML: %w", err)
		}
	}

	return nil
}

func isStructuredMapping(envelope map[string]any) bool {
	if envelope == nil {
		return false
	}
	_, hasFiles := envelope["files"]
	_, hasDefaults := envelope["defaults"]
	return hasFiles || hasDefaults
}

func isSupportedStandardField(field string) bool {
	_, ok := supportedStandardFields[strings.TrimSpace(field)]
	return ok
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}

	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
