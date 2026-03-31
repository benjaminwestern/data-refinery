// Package input provides a shared document/record reader abstraction for
// workflow consumers that need to read structured input in a common way.
package input

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/benjaminwestern/data-refinery/internal/source"
)

// Source is the minimal read contract required by the input package.
//
// source.InputSource satisfies this interface, so callers can pass either the
// concrete source package type or a lightweight test double.
type Source interface {
	Path() string
	Open(ctx context.Context) (io.ReadCloser, error)
}

// Format identifies the parsed input encoding.
type Format string

const (
	// FormatUnknown is returned when the format cannot be determined.
	FormatUnknown Format = "unknown"
	// FormatCSV represents comma-separated tabular records.
	FormatCSV Format = "csv"
	// FormatTSV represents tab-separated tabular records.
	FormatTSV Format = "tsv"
	// FormatXLSX represents spreadsheet rows from an Excel workbook.
	FormatXLSX Format = "xlsx"
	// FormatXML represents XML documents normalized into nested record data.
	FormatXML Format = "xml"
	// FormatJSON represents a top-level JSON object or array.
	FormatJSON Format = "json"
	// FormatNDJSON represents newline-delimited JSON records.
	FormatNDJSON Format = "ndjson"
	// FormatJSONL represents JSON Lines records.
	FormatJSONL Format = "jsonl"
)

// Layout identifies how records are organized in the source payload.
type Layout string

const (
	// LayoutUnknown indicates that the record layout is not known.
	LayoutUnknown Layout = "unknown"
	// LayoutStream indicates line-oriented records.
	LayoutStream Layout = "stream"
	// LayoutObject indicates a single JSON object payload.
	LayoutObject Layout = "object"
	// LayoutArray indicates a JSON array of objects.
	LayoutArray Layout = "array"
)

// JSONMode determines how `.json` inputs are interpreted by the shared reader.
type JSONMode string

const (
	// JSONModeDocument reads `.json` inputs as a single object or array payload.
	JSONModeDocument JSONMode = "document"
	// JSONModeLineStream reads `.json` inputs as line-oriented records.
	JSONModeLineStream JSONMode = "line_stream"
)

// Record represents one logical record read from an input source.
type Record struct {
	SourcePath  string
	RecordIndex int
	LineNumber  int
	Raw         []byte
	Data        map[string]any
	DecodeErr   error
}

// Reader exposes a sequential record stream.
type Reader interface {
	Format() Format
	Layout() Layout
	Next(ctx context.Context) (Record, error)
	Close() error
}

type readerConfig struct {
	jsonMode             JSONMode
	decodeErrorsAsRecord bool
	bufferSize           int
	maxBufferSize        int
	sheet                string
	xmlRecordPath        string
}

// ReaderOption configures reader construction.
type ReaderOption func(*readerConfig)

// WithJSONMode selects how `.json` inputs are interpreted by the shared reader.
func WithJSONMode(mode JSONMode) ReaderOption {
	return func(cfg *readerConfig) {
		cfg.jsonMode = mode
	}
}

// WithScanBuffer overrides the scanner buffer sizes used by line-oriented
// readers. Values less than or equal to zero fall back to package defaults.
func WithScanBuffer(bufferSize, maxBufferSize int) ReaderOption {
	return func(cfg *readerConfig) {
		cfg.bufferSize = bufferSize
		cfg.maxBufferSize = maxBufferSize
	}
}

// WithSheet selects the workbook sheet to read when the source format is XLSX.
// When omitted, the first sheet is used.
func WithSheet(sheet string) ReaderOption {
	return func(cfg *readerConfig) {
		cfg.sheet = strings.TrimSpace(sheet)
	}
}

// WithXMLRecordPath selects the XML element path to expose as logical records.
// When omitted, the whole XML document is returned as a single record.
func WithXMLRecordPath(path string) ReaderOption {
	return func(cfg *readerConfig) {
		cfg.xmlRecordPath = strings.TrimSpace(path)
	}
}

// WithDecodeErrorsAsRecords preserves malformed line-oriented JSON-family
// records as Record values with DecodeErr set instead of aborting the stream.
func WithDecodeErrorsAsRecords() ReaderOption {
	return func(cfg *readerConfig) {
		cfg.decodeErrorsAsRecord = true
	}
}

// DetectFormatFromPath infers the format from a file extension.
func DetectFormatFromPath(path string) Format {
	switch strings.ToLower(filepath.Ext(strings.TrimSpace(path))) {
	case ".csv":
		return FormatCSV
	case ".tsv":
		return FormatTSV
	case ".xlsx":
		return FormatXLSX
	case ".xml":
		return FormatXML
	case ".json":
		return FormatJSON
	case ".ndjson":
		return FormatNDJSON
	case ".jsonl":
		return FormatJSONL
	default:
		return FormatUnknown
	}
}

// NormalizeFormat canonicalizes accepted format aliases.
func NormalizeFormat(format string) Format {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case string(FormatCSV):
		return FormatCSV
	case string(FormatTSV):
		return FormatTSV
	case string(FormatXLSX):
		return FormatXLSX
	case string(FormatXML):
		return FormatXML
	case string(FormatJSON):
		return FormatJSON
	case string(FormatNDJSON):
		return FormatNDJSON
	case string(FormatJSONL):
		return FormatJSONL
	default:
		return FormatUnknown
	}
}

// NewReader opens a source and returns a record reader for its detected format.
func NewReader(ctx context.Context, src Source, opts ...ReaderOption) (Reader, error) {
	if src == nil {
		return nil, fmt.Errorf("source cannot be nil")
	}

	cfg := readerConfig{
		jsonMode: JSONModeDocument,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	format := DetectFormatFromPath(src.Path())
	if format == FormatUnknown {
		return nil, fmt.Errorf("unsupported input format for %s", src.Path())
	}

	reader, err := src.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open input source %s: %w", src.Path(), err)
	}

	return NewReaderFromReadCloser(src.Path(), reader, format, opts...)
}

// NewReaderFromSource is a convenience helper for source.InputSource values.
func NewReaderFromSource(ctx context.Context, src source.InputSource, opts ...ReaderOption) (Reader, error) {
	if src == nil {
		return nil, fmt.Errorf("source cannot be nil")
	}
	return NewReader(ctx, src, opts...)
}

// NewReaderFromReadCloser creates a reader for an already-opened stream.
func NewReaderFromReadCloser(sourcePath string, reader io.ReadCloser, format Format, opts ...ReaderOption) (Reader, error) {
	cfg := readerConfig{
		jsonMode: JSONModeDocument,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}

	switch format {
	case FormatUnknown:
		safeClose(reader)
		return nil, fmt.Errorf("unsupported input format %q", format)
	case FormatCSV:
		return newDelimitedReader(sourcePath, reader, format, ',', cfg.bufferSize, cfg.maxBufferSize)
	case FormatTSV:
		return newDelimitedReader(sourcePath, reader, format, '\t', cfg.bufferSize, cfg.maxBufferSize)
	case FormatXLSX:
		return newSpreadsheetReader(sourcePath, reader, cfg.sheet)
	case FormatXML:
		return newXMLReader(sourcePath, reader, cfg.xmlRecordPath)
	case FormatJSON:
		if cfg.jsonMode == JSONModeLineStream {
			return newJSONLinesReader(sourcePath, reader, format, cfg.bufferSize, cfg.maxBufferSize, cfg.decodeErrorsAsRecord)
		}
		return newJSONReader(sourcePath, reader)
	case FormatNDJSON, FormatJSONL:
		return newJSONLinesReader(sourcePath, reader, format, cfg.bufferSize, cfg.maxBufferSize, cfg.decodeErrorsAsRecord)
	default:
		safeClose(reader)
		return nil, fmt.Errorf("unsupported input format %q", format)
	}
}

// ReadAll consumes every record from the source and returns them as a slice.
func ReadAll(ctx context.Context, src Source, opts ...ReaderOption) ([]Record, error) {
	reader, err := NewReader(ctx, src, opts...)
	if err != nil {
		return nil, fmt.Errorf("open reader for %s: %w", src.Path(), err)
	}
	defer safeClose(reader)

	var records []Record
	for {
		record, err := reader.Next(ctx)
		if err == io.EOF {
			return records, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read record from %s: %w", src.Path(), err)
		}
		records = append(records, record)
	}
}

func safeClose(closer io.Closer) {
	if closer == nil {
		return
	}
	_ = closer.Close()
}
