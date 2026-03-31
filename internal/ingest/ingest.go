package ingest

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xuri/excelize/v2"

	"github.com/benjaminwestern/data-refinery/internal/output"
	jsonpath "github.com/benjaminwestern/data-refinery/internal/path"
	"github.com/benjaminwestern/data-refinery/internal/safety"
	"github.com/benjaminwestern/data-refinery/internal/source"
)

const (
	defaultScanBuffer = 1024 * 1024
	maxScanBuffer     = 10 * 1024 * 1024
)

// Summary captures the outcome of an ingest run.
type Summary struct {
	OutputPath      string      `json:"outputPath"`
	LogPath         string      `json:"logPath"`
	StatsOutputPath string      `json:"statsOutputPath"`
	FilesDiscovered int         `json:"filesDiscovered"`
	FilesProcessed  int         `json:"filesProcessed"`
	FilesSkipped    int         `json:"filesSkipped"`
	FilesFailed     int         `json:"filesFailed"`
	RowsWritten     int64       `json:"rowsWritten"`
	GeneratedAt     string      `json:"generatedAt"`
	Files           []FileStats `json:"files"`
}

// FileStats captures per-source ingest statistics.
type FileStats struct {
	SourcePath   string   `json:"sourcePath"`
	SourceFile   string   `json:"sourceFile"`
	Format       string   `json:"format"`
	Status       string   `json:"status"`
	RowsRead     int64    `json:"rowsRead"`
	RowsWritten  int64    `json:"rowsWritten"`
	Warnings     []string `json:"warnings,omitempty"`
	ErrorMessage string   `json:"errorMessage,omitempty"`
	StartedAt    string   `json:"startedAt"`
	CompletedAt  string   `json:"completedAt"`
}

type unifiedRecord struct {
	ID                 *string            `json:"Id"`
	FirstName          *string            `json:"FirstName"`
	LastName           *string            `json:"LastName"`
	Email              *string            `json:"Email"`
	Mobile             *string            `json:"Mobile"`
	PostCode           *string            `json:"PostCode"`
	DataSource         *string            `json:"DataSource"`
	SourceCreatedDate  *string            `json:"SourceCreatedDate"`
	SourceModifiedDate *string            `json:"SourceModifiedDate"`
	SourceFile         string             `json:"SourceFile"`
	Attributes         []unifiedAttribute `json:"Attributes"`
	BQInsertedDate     string             `json:"BQInsertedDate"`
}

type unifiedAttribute struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type fileResult struct {
	stats FileStats
	err   error
}

type recordHandler func(map[string]any) error

type outputFormat string

const (
	outputFormatJSONL outputFormat = "jsonl"
	outputFormatJSON  outputFormat = "json"
	outputFormatCSV   outputFormat = "csv"
	inputFormatCSV                 = string(outputFormatCSV)
	inputFormatTSV                 = "tsv"
	inputFormatXLSX                = "xlsx"
	inputFormatJSON                = string(outputFormatJSON)
	inputFormatJSONL               = string(outputFormatJSONL)
	fileStatusSuccess              = "success"
	fileStatusSkipped              = "skipped"
	fileStatusFailed               = "failed"
)

var csvOutputHeaders = []string{
	"Id",
	"FirstName",
	"LastName",
	"Email",
	"Mobile",
	"PostCode",
	"DataSource",
	"SourceCreatedDate",
	"SourceModifiedDate",
	"SourceFile",
	"BQInsertedDate",
}

type unifiedWriter struct {
	mu          sync.Mutex
	ctx         context.Context
	targetPath  string
	format      outputFormat
	localFile   *os.File
	localWriter *bufio.Writer
	gcsWriter   *output.GCSWriter
	csvWriter   *csv.Writer
	tempPath    string
	failed      bool
	wroteRecord bool
}

type modTimeProvider interface {
	ModTime() time.Time
}

// Run executes the ingest workflow and writes a unified output dataset.
func Run(ctx context.Context, cfg *Config) (*Summary, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	mappings, err := LoadMappingSet(cfg.MappingFile)
	if err != nil {
		return nil, fmt.Errorf("load mapping file: %w", err)
	}

	sources, err := source.DiscoverAllWithOptions(ctx, cfg.Paths, ingestDiscoveryOptions())
	if err != nil {
		return nil, fmt.Errorf("discover ingest sources: %w", err)
	}

	statsOutputPath := cfg.StatsOutputPath
	if statsOutputPath == "" {
		statsOutputPath = filepath.Join(
			cfg.LogPath,
			fmt.Sprintf("ingest_summary_%s.json", time.Now().UTC().Format("2006-01-02_15-04-05")),
		)
	}

	writer, err := newUnifiedWriter(ctx, cfg.OutputPath)
	if err != nil {
		return nil, fmt.Errorf("create unified output writer: %w", err)
	}

	summary := &Summary{
		OutputPath:      cfg.OutputPath,
		LogPath:         cfg.LogPath,
		StatsOutputPath: statsOutputPath,
		FilesDiscovered: len(sources),
		GeneratedAt:     time.Now().UTC().Format(time.RFC3339),
	}

	sourceCh := make(chan source.InputSource)
	resultCh := make(chan fileResult, len(sources))

	var wg sync.WaitGroup
	for worker := 0; worker < cfg.Workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for src := range sourceCh {
				stats, err := processSource(ctx, writer, mappings, cfg.RequireMappings, src)
				resultCh <- fileResult{stats: stats, err: err}
			}
		}()
	}

	go func() {
		defer close(sourceCh)
		for _, src := range sources {
			select {
			case sourceCh <- src:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var failures []string
	for result := range resultCh {
		summary.Files = append(summary.Files, result.stats)
		switch result.stats.Status {
		case fileStatusSuccess:
			summary.FilesProcessed++
			summary.RowsWritten += result.stats.RowsWritten
		case fileStatusSkipped:
			summary.FilesSkipped++
		case fileStatusFailed:
			summary.FilesProcessed++
			summary.FilesFailed++
		}
		if result.err != nil {
			failures = append(failures, result.err.Error())
		}
	}

	if closeErr := writer.Close(); closeErr != nil {
		failures = append(failures, closeErr.Error())
	}
	if ctx.Err() != nil {
		failures = append(failures, ctx.Err().Error())
	}

	sort.Slice(summary.Files, func(i, j int) bool {
		return summary.Files[i].SourcePath < summary.Files[j].SourcePath
	})

	if err := writeSummary(ctx, summary.StatsOutputPath, summary); err != nil {
		failures = append(failures, err.Error())
	}

	if len(failures) > 0 {
		return summary, fmt.Errorf("%s", strings.Join(failures, "; "))
	}

	return summary, nil
}

func processSource(ctx context.Context, writer *unifiedWriter, mappings *MappingSet, requireMappings bool, src source.InputSource) (FileStats, error) {
	stats := FileStats{
		SourcePath: src.Path(),
		SourceFile: filepath.Base(src.Path()),
		StartedAt:  time.Now().UTC().Format(time.RFC3339),
		Status:     fileStatusSuccess,
	}
	defer func() {
		stats.CompletedAt = time.Now().UTC().Format(time.RFC3339)
	}()

	format := inferInputFormat(src.Path())
	stats.Format = format
	if format == "" {
		stats.Status = fileStatusFailed
		stats.ErrorMessage = "unsupported input format"
		return stats, fmt.Errorf("unsupported input format for %s", src.Path())
	}

	mapping := mappings.Resolve(src.Path(), format)
	if mapping == nil {
		if requireMappings {
			stats.Status = fileStatusFailed
			stats.ErrorMessage = "no mapping configuration matched this file"
			return stats, fmt.Errorf("no mapping matched %s", src.Path())
		}

		stats.Status = fileStatusSkipped
		stats.Warnings = []string{"no mapping configuration matched this file"}
		return stats, nil
	}

	reader, err := src.Open(ctx)
	if err != nil {
		stats.Status = fileStatusFailed
		stats.ErrorMessage = err.Error()
		return stats, fmt.Errorf("open %s: %w", src.Path(), err)
	}
	defer safety.Close(reader, src.Path())

	ingestTime := time.Now().UTC()
	warnings := make(map[string]struct{})
	rowCounter := atomic.Int64{}

	handleRecord := func(row map[string]any) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rowCounter.Add(1)
		record, recordWarnings, err := normalizeRecord(row, src, mapping, ingestTime)
		if err != nil {
			return err
		}
		for _, warning := range recordWarnings {
			warnings[warning] = struct{}{}
		}
		if err := writer.WriteRecord(record); err != nil {
			return err
		}
		stats.RowsWritten++
		return nil
	}

	if err := streamInputRecords(ctx, reader, format, mapping.Sheet, handleRecord); err != nil {
		stats.Status = fileStatusFailed
		stats.RowsRead = rowCounter.Load()
		stats.ErrorMessage = err.Error()
		stats.Warnings = sortedWarningValues(warnings)
		return stats, fmt.Errorf("process %s: %w", src.Path(), err)
	}

	stats.RowsRead = rowCounter.Load()
	stats.Warnings = sortedWarningValues(warnings)
	return stats, nil
}

func normalizeRecord(row map[string]any, src source.InputSource, mapping *ResolvedFileMapping, ingestedAt time.Time) (unifiedRecord, []string, error) {
	record := unifiedRecord{
		SourceFile:     filepath.Base(src.Path()),
		Attributes:     make([]unifiedAttribute, 0),
		BQInsertedDate: ingestedAt.Format(time.RFC3339),
	}

	nullValues := makeNullLookup(mapping.EffectiveNullValues)
	usedKeys := make(map[string]struct{})
	var warnings []string

	if modTime := sourceModifiedTime(src); !modTime.IsZero() {
		modified := modTime.UTC().Format(time.RFC3339)
		record.SourceModifiedDate = &modified
	}

	if mapping.EffectiveDataSource != "" {
		if normalized, ok := normalizeValue(mapping.EffectiveDataSource, mapping.EffectiveTrimWhitespace, nullValues); ok {
			record.DataSource = normalized
		}
	}

	defaultFields := sortedMapKeys(mapping.Defaults)
	for _, targetField := range defaultFields {
		normalized, ok := normalizeValue(mapping.Defaults[targetField], mapping.EffectiveTrimWhitespace, nullValues)
		if !ok {
			continue
		}
		if err := setStandardField(&record, targetField, normalized); err != nil {
			return unifiedRecord{}, nil, err
		}
	}

	if mapping.SourceCreatedDateField != "" {
		value, usedKey, ok := lookupValue(row, mapping.SourceCreatedDateField)
		if !ok {
			warnings = append(warnings, fmt.Sprintf("source field %q was not found", mapping.SourceCreatedDateField))
		} else if normalized, keep := normalizeValue(value, mapping.EffectiveTrimWhitespace, nullValues); keep {
			record.SourceCreatedDate = normalized
			usedKeys[usedKey] = struct{}{}
		}
	}

	if mapping.SourceModifiedDateField != "" {
		value, usedKey, ok := lookupValue(row, mapping.SourceModifiedDateField)
		if !ok {
			warnings = append(warnings, fmt.Sprintf("source field %q was not found", mapping.SourceModifiedDateField))
		} else if normalized, keep := normalizeValue(value, mapping.EffectiveTrimWhitespace, nullValues); keep {
			record.SourceModifiedDate = normalized
			usedKeys[usedKey] = struct{}{}
		}
	}

	columnFields := sortedMapKeys(mapping.Columns)
	for _, sourceField := range columnFields {
		targetField := mapping.Columns[sourceField]
		value, usedKey, ok := lookupValue(row, sourceField)
		if !ok {
			warnings = append(warnings, fmt.Sprintf("source field %q was not found", sourceField))
			continue
		}
		usedKeys[usedKey] = struct{}{}

		normalized, keep := normalizeValue(value, mapping.EffectiveTrimWhitespace, nullValues)
		if !keep {
			continue
		}
		if err := setStandardField(&record, targetField, normalized); err != nil {
			return unifiedRecord{}, nil, err
		}
	}

	attributeFields := sortedMapKeys(mapping.Attributes)
	for _, sourceField := range attributeFields {
		attributeKey := mapping.Attributes[sourceField]
		value, usedKey, ok := lookupValue(row, sourceField)
		if !ok {
			warnings = append(warnings, fmt.Sprintf("source field %q was not found", sourceField))
			continue
		}
		usedKeys[usedKey] = struct{}{}

		normalized, keep := normalizeValue(value, mapping.EffectiveTrimWhitespace, nullValues)
		if !keep {
			continue
		}
		record.Attributes = append(record.Attributes, unifiedAttribute{
			Key:   attributeKey,
			Value: *normalized,
		})
	}

	if mapping.EffectiveIncludeUnmapped {
		for _, key := range sortedMapKeysAny(row) {
			if _, alreadyUsed := usedKeys[key]; alreadyUsed {
				continue
			}
			normalized, keep := normalizeValue(row[key], mapping.EffectiveTrimWhitespace, nullValues)
			if !keep {
				continue
			}
			record.Attributes = append(record.Attributes, unifiedAttribute{
				Key:   key,
				Value: *normalized,
			})
		}
	}

	sort.Slice(record.Attributes, func(i, j int) bool {
		if record.Attributes[i].Key == record.Attributes[j].Key {
			return record.Attributes[i].Value < record.Attributes[j].Value
		}
		return record.Attributes[i].Key < record.Attributes[j].Key
	})

	return record, warnings, nil
}

func streamInputRecords(ctx context.Context, reader io.Reader, format, sheet string, handle recordHandler) error {
	switch format {
	case inputFormatCSV:
		return streamDelimitedRecords(ctx, reader, ',', handle)
	case inputFormatTSV:
		return streamDelimitedRecords(ctx, reader, '\t', handle)
	case inputFormatXLSX:
		return streamSpreadsheetRecords(ctx, reader, sheet, handle)
	case inputFormatJSON:
		return streamJSONRecords(ctx, reader, handle)
	case inputFormatJSONL:
		return streamJSONLines(ctx, reader, handle)
	default:
		return fmt.Errorf("unsupported input format %q", format)
	}
}

func streamDelimitedRecords(ctx context.Context, reader io.Reader, delimiter rune, handle recordHandler) error {
	csvReader := csv.NewReader(reader)
	csvReader.Comma = delimiter
	csvReader.FieldsPerRecord = -1

	headers, err := csvReader.Read()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read header row: %w", err)
	}
	headers = normalizeHeaders(headers)

	for {
		if ctx.Err() != nil {
			return fmt.Errorf("stream delimited records: %w", ctx.Err())
		}

		record, err := csvReader.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read delimited row: %w", err)
		}

		row := make(map[string]any, len(headers))
		for index, header := range headers {
			value := ""
			if index < len(record) {
				value = record[index]
			}
			row[header] = value
		}
		if !rowHasValues(row) {
			continue
		}
		if err := handle(row); err != nil {
			return err
		}
	}
}

func streamSpreadsheetRecords(ctx context.Context, reader io.Reader, sheet string, handle recordHandler) error {
	workbook, err := excelize.OpenReader(reader)
	if err != nil {
		return fmt.Errorf("open workbook: %w", err)
	}
	defer safety.Close(workbook, "workbook")

	targetSheet := sheet
	if targetSheet == "" {
		sheets := workbook.GetSheetList()
		if len(sheets) == 0 {
			return fmt.Errorf("workbook does not contain any sheets")
		}
		targetSheet = sheets[0]
	}

	rows, err := workbook.Rows(targetSheet)
	if err != nil {
		return fmt.Errorf("open sheet %q: %w", targetSheet, err)
	}
	defer safety.Close(rows, targetSheet)

	var headers []string
	for rows.Next() {
		if ctx.Err() != nil {
			return fmt.Errorf("stream spreadsheet records: %w", ctx.Err())
		}

		columns, err := rows.Columns()
		if err != nil {
			return fmt.Errorf("read spreadsheet row: %w", err)
		}

		if headers == nil {
			headers = normalizeHeaders(columns)
			continue
		}

		row := make(map[string]any, len(headers))
		for index, header := range headers {
			value := ""
			if index < len(columns) {
				value = columns[index]
			}
			row[header] = value
		}
		if !rowHasValues(row) {
			continue
		}
		if err := handle(row); err != nil {
			return err
		}
	}

	return nil
}

func streamJSONLines(ctx context.Context, reader io.Reader, handle recordHandler) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, defaultScanBuffer), maxScanBuffer)

	lineNumber := 0
	for scanner.Scan() {
		if ctx.Err() != nil {
			return fmt.Errorf("stream JSON lines: %w", ctx.Err())
		}
		lineNumber++

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var row map[string]any
		decoder := json.NewDecoder(strings.NewReader(line))
		decoder.UseNumber()
		if err := decoder.Decode(&row); err != nil {
			return fmt.Errorf("decode JSONL line %d: %w", lineNumber, err)
		}

		if err := handle(row); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan JSONL input: %w", err)
	}

	return nil
}

func streamJSONRecords(ctx context.Context, reader io.Reader, handle recordHandler) error {
	buffered := bufio.NewReader(reader)
	firstByte, err := peekNonWhitespace(buffered)
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect JSON input: %w", err)
	}

	decoder := json.NewDecoder(buffered)
	decoder.UseNumber()

	if firstByte != '[' {
		var row map[string]any
		if err := decoder.Decode(&row); err != nil {
			return fmt.Errorf("decode JSON object: %w", err)
		}
		return handle(row)
	}

	return streamJSONArrayRecords(ctx, decoder, handle)
}

func newUnifiedWriter(ctx context.Context, targetPath string) (*unifiedWriter, error) {
	writer := &unifiedWriter{
		ctx:        ctx,
		targetPath: targetPath,
		format:     outputFormatFromPath(targetPath),
	}

	switch writer.format {
	case outputFormatCSV:
		return newCSVUnifiedWriter(writer)
	case outputFormatJSON:
		return newJSONUnifiedWriter(writer)
	case outputFormatJSONL:
		return newJSONLUnifiedWriter(writer)
	}

	return nil, fmt.Errorf("unsupported ingest output format: %s", writer.format)
}

func (w *unifiedWriter) WriteRecord(record unifiedRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	switch w.format {
	case outputFormatCSV:
		return w.writeCSVRecord(record)
	case outputFormatJSON:
		return w.writeJSONRecord(record)
	case outputFormatJSONL:
		return w.writeJSONLRecord(record)
	}

	return fmt.Errorf("unsupported ingest output format: %s", w.format)
}

func (w *unifiedWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	switch w.format {
	case outputFormatCSV:
		return w.closeCSVWriter()
	case outputFormatJSON:
		return w.closeJSONWriter()
	case outputFormatJSONL:
		return w.closeJSONLWriter()
	}

	return fmt.Errorf("unsupported ingest output format: %s", w.format)
}

func newJSONLUnifiedWriter(writer *unifiedWriter) (*unifiedWriter, error) {
	if isRemotePath(writer.targetPath) {
		gcsWriter, err := output.NewGCSWriter(writer.ctx, writer.targetPath)
		if err != nil {
			return nil, fmt.Errorf("create GCS output writer: %w", err)
		}
		if err := gcsWriter.Open(); err != nil {
			return nil, fmt.Errorf("open GCS output writer: %w", err)
		}
		writer.gcsWriter = gcsWriter
		return writer, nil
	}

	if err := os.MkdirAll(filepath.Dir(writer.targetPath), 0o700); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}

	file, err := os.OpenFile(writer.targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open output file: %w", err)
	}
	writer.localFile = file
	writer.localWriter = bufio.NewWriter(file)
	return writer, nil
}

func newCSVUnifiedWriter(writer *unifiedWriter) (*unifiedWriter, error) {
	tempDir := ""
	if !isRemotePath(writer.targetPath) {
		tempDir = filepath.Dir(writer.targetPath)
		if err := os.MkdirAll(tempDir, 0o700); err != nil {
			return nil, fmt.Errorf("create output directory: %w", err)
		}
	}

	tempFile, err := os.CreateTemp(tempDir, "data-refinery-ingest-*.csv")
	if err != nil {
		return nil, fmt.Errorf("create temporary CSV output file: %w", err)
	}
	writer.localFile = tempFile
	writer.tempPath = tempFile.Name()
	writer.csvWriter = csv.NewWriter(tempFile)
	if err := writer.csvWriter.Write(csvOutputHeaders); err != nil {
		safety.Close(tempFile, writer.tempPath)
		safety.Remove(writer.tempPath, writer.tempPath)
		return nil, fmt.Errorf("write CSV header: %w", err)
	}

	return writer, nil
}

func newJSONUnifiedWriter(writer *unifiedWriter) (*unifiedWriter, error) {
	tempDir := ""
	if !isRemotePath(writer.targetPath) {
		tempDir = filepath.Dir(writer.targetPath)
		if err := os.MkdirAll(tempDir, 0o700); err != nil {
			return nil, fmt.Errorf("create output directory: %w", err)
		}
	}

	tempFile, err := os.CreateTemp(tempDir, "data-refinery-ingest-*.json")
	if err != nil {
		return nil, fmt.Errorf("create temporary JSON output file: %w", err)
	}
	writer.localFile = tempFile
	writer.localWriter = bufio.NewWriter(tempFile)
	writer.tempPath = tempFile.Name()
	if _, err := writer.localWriter.WriteString("["); err != nil {
		safety.Close(tempFile, writer.tempPath)
		safety.Remove(writer.tempPath, writer.tempPath)
		return nil, fmt.Errorf("write JSON array prefix: %w", err)
	}

	return writer, nil
}

func (w *unifiedWriter) writeJSONLRecord(record unifiedRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal unified record: %w", err)
	}

	if w.localWriter != nil {
		if _, err := w.localWriter.Write(data); err != nil {
			return fmt.Errorf("write output record: %w", err)
		}
		if err := w.localWriter.WriteByte('\n'); err != nil {
			return fmt.Errorf("write output newline: %w", err)
		}
		return nil
	}

	if w.gcsWriter != nil {
		if _, err := w.gcsWriter.WriteLine(string(data)); err != nil {
			return fmt.Errorf("write GCS output record: %w", err)
		}
		return nil
	}

	return fmt.Errorf("writer is not initialized")
}

func (w *unifiedWriter) writeCSVRecord(record unifiedRecord) error {
	if len(record.Attributes) > 0 {
		w.failed = true
		return fmt.Errorf(
			"CSV output does not support complex nested fields such as Attributes; choose .json, .ndjson, or .jsonl instead",
		)
	}

	if w.csvWriter == nil {
		w.failed = true
		return fmt.Errorf("csv writer is not initialized")
	}

	if err := w.csvWriter.Write(record.toCSVRow()); err != nil {
		w.failed = true
		return fmt.Errorf("write CSV output record: %w", err)
	}

	return nil
}

func (w *unifiedWriter) writeJSONRecord(record unifiedRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		w.failed = true
		return fmt.Errorf("marshal unified record: %w", err)
	}
	if w.localWriter == nil {
		w.failed = true
		return fmt.Errorf("json writer is not initialized")
	}

	prefix := "\n"
	if w.wroteRecord {
		prefix = ",\n"
	}
	if _, err := w.localWriter.WriteString(prefix); err != nil {
		w.failed = true
		return fmt.Errorf("write JSON output delimiter: %w", err)
	}
	if _, err := w.localWriter.Write(data); err != nil {
		w.failed = true
		return fmt.Errorf("write JSON output record: %w", err)
	}
	w.wroteRecord = true
	return nil
}

func (w *unifiedWriter) closeJSONLWriter() error {
	if w.localWriter != nil {
		if err := w.localWriter.Flush(); err != nil {
			return fmt.Errorf("flush JSONL output writer: %w", err)
		}
		w.localWriter = nil
	}
	if w.localFile != nil {
		if err := w.localFile.Close(); err != nil {
			return fmt.Errorf("close JSONL output file: %w", err)
		}
		w.localFile = nil
	}
	if w.gcsWriter != nil {
		if err := w.gcsWriter.Close(); err != nil {
			return fmt.Errorf("close JSONL GCS output writer: %w", err)
		}
		w.gcsWriter = nil
	}

	return nil
}

func (w *unifiedWriter) closeCSVWriter() error {
	if w.csvWriter != nil {
		w.csvWriter.Flush()
		if err := w.csvWriter.Error(); err != nil {
			w.failed = true
			return fmt.Errorf("flush CSV output writer: %w", err)
		}
		w.csvWriter = nil
	}
	if w.localFile != nil {
		if err := w.localFile.Close(); err != nil {
			return fmt.Errorf("close CSV temp output file: %w", err)
		}
		w.localFile = nil
	}

	if w.tempPath == "" {
		return nil
	}
	defer safety.Remove(w.tempPath, w.tempPath)

	if w.failed {
		return nil
	}

	if isRemotePath(w.targetPath) {
		return copyFileToPath(w.ctx, w.tempPath, w.targetPath)
	}

	if err := os.MkdirAll(filepath.Dir(w.targetPath), 0o700); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	if err := os.Rename(w.tempPath, w.targetPath); err != nil {
		return fmt.Errorf("commit CSV output file: %w", err)
	}
	w.tempPath = ""
	return nil
}

func (w *unifiedWriter) closeJSONWriter() error {
	if w.localWriter != nil {
		suffix := "]\n"
		if w.wroteRecord {
			suffix = "\n]\n"
		}
		if _, err := w.localWriter.WriteString(suffix); err != nil {
			w.failed = true
			return fmt.Errorf("write JSON output suffix: %w", err)
		}
		if err := w.localWriter.Flush(); err != nil {
			w.failed = true
			return fmt.Errorf("flush JSON output writer: %w", err)
		}
		w.localWriter = nil
	}
	if w.localFile != nil {
		if err := w.localFile.Close(); err != nil {
			return fmt.Errorf("close JSON temp output file: %w", err)
		}
		w.localFile = nil
	}

	if w.tempPath == "" {
		return nil
	}
	defer safety.Remove(w.tempPath, w.tempPath)

	if w.failed {
		return nil
	}

	if isRemotePath(w.targetPath) {
		return copyFileToPath(w.ctx, w.tempPath, w.targetPath)
	}

	if err := os.MkdirAll(filepath.Dir(w.targetPath), 0o700); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	if err := os.Rename(w.tempPath, w.targetPath); err != nil {
		return fmt.Errorf("commit JSON output file: %w", err)
	}
	w.tempPath = ""
	return nil
}

func writeSummary(ctx context.Context, targetPath string, summary *Summary) error {
	data, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal ingest summary: %w", err)
	}
	return writeBytesToPath(ctx, targetPath, data)
}

func writeBytesToPath(ctx context.Context, targetPath string, data []byte) error {
	if isRemotePath(targetPath) {
		gcsWriter, err := output.NewGCSWriter(ctx, targetPath)
		if err != nil {
			return fmt.Errorf("create GCS writer: %w", err)
		}
		defer safety.Close(gcsWriter, targetPath)

		if err := gcsWriter.Open(); err != nil {
			return fmt.Errorf("open GCS writer: %w", err)
		}
		if _, err := gcsWriter.Write(data); err != nil {
			return fmt.Errorf("write GCS object: %w", err)
		}
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(targetPath), 0o700); err != nil {
		return fmt.Errorf("create summary directory: %w", err)
	}
	if err := os.WriteFile(targetPath, data, 0o600); err != nil {
		return fmt.Errorf("write summary file: %w", err)
	}

	return nil
}

func copyFileToPath(ctx context.Context, localPath, targetPath string) error {
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open staged output file: %w", err)
	}
	defer safety.Close(file, localPath)

	if isRemotePath(targetPath) {
		gcsWriter, err := output.NewGCSWriter(ctx, targetPath)
		if err != nil {
			return fmt.Errorf("create GCS writer: %w", err)
		}
		defer safety.Close(gcsWriter, targetPath)

		if err := gcsWriter.Open(); err != nil {
			return fmt.Errorf("open GCS writer: %w", err)
		}
		if _, err := gcsWriter.StreamCopy(file); err != nil {
			return fmt.Errorf("stream staged output to GCS: %w", err)
		}
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(targetPath), 0o700); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	destination, err := os.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open destination file: %w", err)
	}
	defer safety.Close(destination, targetPath)

	if _, err := io.Copy(destination, file); err != nil {
		return fmt.Errorf("copy staged output file: %w", err)
	}
	return nil
}

func setStandardField(record *unifiedRecord, field string, value *string) error {
	switch field {
	case "Id":
		record.ID = value
	case "FirstName":
		record.FirstName = value
	case "LastName":
		record.LastName = value
	case "Email":
		record.Email = value
	case "Mobile":
		record.Mobile = value
	case "PostCode":
		record.PostCode = value
	case "DataSource":
		record.DataSource = value
	case "SourceCreatedDate":
		record.SourceCreatedDate = value
	case "SourceModifiedDate":
		record.SourceModifiedDate = value
	default:
		return fmt.Errorf("unsupported target field %q", field)
	}

	return nil
}

func lookupValue(row map[string]any, key string) (any, string, bool) {
	if value, ok := row[key]; ok {
		return value, key, true
	}
	if !strings.ContainsAny(key, ".[") {
		return nil, "", false
	}

	processor := jsonpath.NewJSONPathProcessor()
	components, err := processor.ParsePath(key)
	if err != nil || len(components) == 0 {
		return nil, "", false
	}

	value, ok := resolvePathValue(row, components)
	if !ok {
		return nil, "", false
	}

	return value, components[0].Key, true
}

func resolvePathValue(current any, components []jsonpath.Component) (any, bool) {
	if len(components) == 0 {
		return current, true
	}

	component := components[0]
	object, ok := current.(map[string]any)
	if !ok {
		return nil, false
	}

	value, exists := object[component.Key]
	if !exists {
		return nil, false
	}

	if !component.IsArrayAccess {
		return resolvePathValue(value, components[1:])
	}

	return resolveArrayPathValue(value, component, components[1:])
}

func streamJSONArrayRecords(ctx context.Context, decoder *json.Decoder, handle recordHandler) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("read JSON array token: %w", err)
	}

	delim, ok := token.(json.Delim)
	if !ok || delim != '[' {
		return fmt.Errorf("expected JSON array")
	}

	rowIndex := 0
	for decoder.More() {
		if ctx.Err() != nil {
			return fmt.Errorf("stream JSON array records: %w", ctx.Err())
		}

		var row map[string]any
		if err := decoder.Decode(&row); err != nil {
			return fmt.Errorf("decode JSON array row %d: %w", rowIndex+1, err)
		}
		if err := handle(row); err != nil {
			return err
		}
		rowIndex++
	}

	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("close JSON array: %w", err)
	}

	return nil
}

func resolveArrayPathValue(value any, component jsonpath.Component, remaining []jsonpath.Component) (any, bool) {
	array, ok := value.([]any)
	if !ok {
		return nil, false
	}

	if component.IsWildcard {
		for _, item := range array {
			resolved, ok := resolvePathValue(item, remaining)
			if ok {
				return resolved, true
			}
		}
		return nil, false
	}

	if component.ArrayIndex < 0 || component.ArrayIndex >= len(array) {
		return nil, false
	}

	return resolvePathValue(array[component.ArrayIndex], remaining)
}

func normalizeValue(value any, trimWhitespace bool, nullValues map[string]struct{}) (*string, bool) {
	if value == nil {
		return nil, false
	}

	var normalized string
	switch typed := value.(type) {
	case string:
		normalized = typed
	case json.Number:
		normalized = typed.String()
	case float64:
		normalized = strconv.FormatFloat(typed, 'f', -1, 64)
	case float32:
		normalized = strconv.FormatFloat(float64(typed), 'f', -1, 32)
	case int:
		normalized = strconv.Itoa(typed)
	case int8, int16, int32, int64:
		normalized = fmt.Sprintf("%d", typed)
	case uint, uint8, uint16, uint32, uint64:
		normalized = fmt.Sprintf("%d", typed)
	case bool:
		normalized = strconv.FormatBool(typed)
	default:
		jsonValue, err := json.Marshal(typed)
		if err == nil && string(jsonValue) != "null" {
			normalized = string(jsonValue)
		} else {
			normalized = fmt.Sprint(typed)
		}
	}

	if trimWhitespace {
		normalized = strings.TrimSpace(normalized)
	}
	if _, isNull := nullValues[strings.ToLower(normalized)]; isNull {
		return nil, false
	}

	return &normalized, true
}

func makeNullLookup(values []string) map[string]struct{} {
	lookup := make(map[string]struct{}, len(values))
	for _, value := range values {
		lookup[strings.ToLower(strings.TrimSpace(value))] = struct{}{}
	}
	return lookup
}

func ingestDiscoveryOptions() source.DiscoveryOptions {
	return source.DiscoveryOptions{
		AllowedExtensions: []string{".csv", ".tsv", ".xlsx", ".json", ".ndjson", ".jsonl"},
		AllowedContentTypes: map[string]bool{
			"text/csv":                  true,
			"application/csv":           true,
			"text/tab-separated-values": true,
			"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": true,
			"application/json":           true,
			"application/x-ndjson":       true,
			"application/json-seq":       true,
			"application/jsonlines":      true,
			"application/jsonlines+json": true,
			"application/x-jsonlines":    true,
		},
		Description: "CSV, TSV, XLSX, JSON, NDJSON, or JSONL",
	}
}

func inferInputFormat(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".csv":
		return inputFormatCSV
	case ".tsv":
		return inputFormatTSV
	case ".xlsx":
		return inputFormatXLSX
	case ".json":
		return inputFormatJSON
	case ".ndjson", ".jsonl":
		return inputFormatJSONL
	default:
		return ""
	}
}

func normalizeInputFormat(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case inputFormatCSV:
		return inputFormatCSV
	case inputFormatTSV:
		return inputFormatTSV
	case inputFormatXLSX:
		return inputFormatXLSX
	case inputFormatJSON:
		return inputFormatJSON
	case "ndjson", "jsonl":
		return inputFormatJSONL
	default:
		return ""
	}
}

func outputFormatFromPath(path string) outputFormat {
	extension := strings.ToLower(filepath.Ext(strings.TrimSpace(path)))
	switch extension {
	case ".csv":
		return outputFormatCSV
	case ".json":
		return outputFormatJSON
	case ".ndjson", ".jsonl":
		return outputFormatJSONL
	default:
		return outputFormatJSONL
	}
}

func sourceModifiedTime(src source.InputSource) time.Time {
	provider, ok := src.(modTimeProvider)
	if !ok {
		return time.Time{}
	}
	return provider.ModTime()
}

func sortedMapKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedMapKeysAny(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedWarningValues(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}

	warnings := make([]string, 0, len(values))
	for warning := range values {
		warnings = append(warnings, warning)
	}
	sort.Strings(warnings)
	return warnings
}

func (record unifiedRecord) toCSVRow() []string {
	return []string{
		stringValue(record.ID),
		stringValue(record.FirstName),
		stringValue(record.LastName),
		stringValue(record.Email),
		stringValue(record.Mobile),
		stringValue(record.PostCode),
		stringValue(record.DataSource),
		stringValue(record.SourceCreatedDate),
		stringValue(record.SourceModifiedDate),
		record.SourceFile,
		record.BQInsertedDate,
	}
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func normalizeHeaders(headers []string) []string {
	normalized := make([]string, 0, len(headers))
	for index, header := range headers {
		header = strings.TrimPrefix(header, "\ufeff")
		header = strings.TrimSpace(header)
		if header == "" {
			header = fmt.Sprintf("column_%d", index+1)
		}
		normalized = append(normalized, header)
	}
	return normalized
}

func rowHasValues(row map[string]any) bool {
	for _, value := range row {
		text, ok := normalizeValue(value, true, makeNullLookup([]string{""}))
		if ok && text != nil && *text != "" {
			return true
		}
	}
	return false
}

func peekNonWhitespace(reader *bufio.Reader) (byte, error) {
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, fmt.Errorf("read next non-whitespace byte: %w", err)
		}
		if !isWhitespace(b) {
			if err := reader.UnreadByte(); err != nil {
				return 0, fmt.Errorf("rewind buffered byte: %w", err)
			}
			return b, nil
		}
	}
}

func isWhitespace(b byte) bool {
	switch b {
	case ' ', '\n', '\r', '\t':
		return true
	default:
		return false
	}
}
