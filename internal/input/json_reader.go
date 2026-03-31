package input

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const (
	defaultScanBuffer = 1 * 1024 * 1024
	maxScanBuffer     = 10 * 1024 * 1024
)

type jsonReader struct {
	sourcePath string
	reader     io.ReadCloser
	decoder    *json.Decoder
	format     Format
	layout     Layout

	mode           jsonMode
	emitted        bool
	arrayCompleted bool
	recordIndex    int
}

type jsonMode int

const (
	jsonModeObject jsonMode = iota
	jsonModeArray
)

func newJSONReader(sourcePath string, reader io.ReadCloser) (Reader, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader cannot be nil")
	}

	buffered := bufio.NewReader(reader)
	firstByte, err := peekNonWhitespace(buffered)
	if err != nil {
		safeClose(reader)
		return nil, fmt.Errorf("inspect JSON input: %w", err)
	}

	if firstByte != '{' && firstByte != '[' {
		safeClose(reader)
		return nil, fmt.Errorf("unsupported JSON input at %s", sourcePath)
	}

	decoder := json.NewDecoder(buffered)
	decoder.UseNumber()

	mode := jsonModeObject
	layout := LayoutObject
	if firstByte == '[' {
		mode = jsonModeArray
		layout = LayoutArray
		if token, err := decoder.Token(); err != nil {
			safeClose(reader)
			return nil, fmt.Errorf("read JSON array token: %w", err)
		} else if delim, ok := token.(json.Delim); !ok || delim != '[' {
			safeClose(reader)
			return nil, fmt.Errorf("expected JSON array in %s", sourcePath)
		}
	}

	return &jsonReader{
		sourcePath: sourcePath,
		reader:     reader,
		decoder:    decoder,
		format:     FormatJSON,
		layout:     layout,
		mode:       mode,
	}, nil
}

func newJSONLinesReader(sourcePath string, reader io.ReadCloser, format Format, bufferSize, maxBufferSize int, decodeErrorsAsRecord bool) (Reader, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader cannot be nil")
	}

	scanner := bufio.NewScanner(reader)
	initialBuffer, maxCapacity := normalizeScannerBufferSizes(bufferSize, maxBufferSize)
	scanner.Buffer(make([]byte, 0, initialBuffer), maxCapacity)

	return &jsonLinesReader{
		sourcePath:           sourcePath,
		reader:               reader,
		scanner:              scanner,
		format:               format,
		layout:               LayoutStream,
		decodeErrorsAsRecord: decodeErrorsAsRecord,
	}, nil
}

func (r *jsonReader) Format() Format {
	return r.format
}

func (r *jsonReader) Layout() Layout {
	return r.layout
}

func (r *jsonReader) Next(ctx context.Context) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, fmt.Errorf("read JSON input %s: %w", r.sourcePath, err)
	}

	switch r.mode {
	case jsonModeObject:
		if r.emitted {
			return Record{}, io.EOF
		}

		var data map[string]any
		if err := r.decoder.Decode(&data); err != nil {
			return Record{}, fmt.Errorf("decode JSON object in %s: %w", r.sourcePath, err)
		}
		r.emitted = true

		if err := r.ensureNoTrailingContent(); err != nil {
			return Record{}, err
		}

		return Record{
			SourcePath:  r.sourcePath,
			RecordIndex: 0,
			LineNumber:  1,
			Data:        data,
		}, nil

	case jsonModeArray:
		if r.arrayCompleted {
			return Record{}, io.EOF
		}

		if !r.decoder.More() {
			if err := r.finishArray(); err != nil {
				return Record{}, err
			}
			return Record{}, io.EOF
		}

		var data map[string]any
		if err := r.decoder.Decode(&data); err != nil {
			return Record{}, fmt.Errorf("decode JSON array item %d in %s: %w", r.recordIndex+1, r.sourcePath, err)
		}

		record := Record{
			SourcePath:  r.sourcePath,
			RecordIndex: r.recordIndex,
			LineNumber:  r.recordIndex + 1,
			Data:        data,
		}
		r.recordIndex++
		return record, nil

	default:
		return Record{}, fmt.Errorf("unsupported JSON reader mode for %s", r.sourcePath)
	}
}

func (r *jsonReader) Close() error {
	return closeAndNil(&r.reader)
}

type jsonLinesReader struct {
	sourcePath           string
	reader               io.ReadCloser
	scanner              *bufio.Scanner
	format               Format
	layout               Layout
	lineNumber           int
	recordIndex          int
	decodeErrorsAsRecord bool
}

func (r *jsonLinesReader) Format() Format {
	return r.format
}

func (r *jsonLinesReader) Layout() Layout {
	return r.layout
}

func (r *jsonLinesReader) Next(ctx context.Context) (Record, error) {
	for r.scanner.Scan() {
		r.lineNumber++
		if err := ctx.Err(); err != nil {
			return Record{}, fmt.Errorf("read %s input %s: %w", r.format, r.sourcePath, err)
		}

		raw := append([]byte(nil), r.scanner.Bytes()...)
		line := strings.TrimSpace(string(raw))
		if line == "" {
			continue
		}

		var data map[string]any
		decoder := json.NewDecoder(strings.NewReader(line))
		decoder.UseNumber()
		if err := decoder.Decode(&data); err != nil {
			record := Record{
				SourcePath:  r.sourcePath,
				RecordIndex: r.recordIndex,
				LineNumber:  r.lineNumber,
				Raw:         append([]byte(nil), raw...),
			}
			r.recordIndex++
			if r.decodeErrorsAsRecord {
				record.DecodeErr = fmt.Errorf("decode %s line %d in %s: %w", r.format, r.lineNumber, r.sourcePath, err)
				return record, nil
			}
			return Record{}, fmt.Errorf("decode %s line %d in %s: %w", r.format, r.lineNumber, r.sourcePath, err)
		}

		record := Record{
			SourcePath:  r.sourcePath,
			RecordIndex: r.recordIndex,
			LineNumber:  r.lineNumber,
			Raw:         append([]byte(nil), raw...),
			Data:        data,
		}
		r.recordIndex++
		return record, nil
	}

	if err := r.scanner.Err(); err != nil {
		return Record{}, fmt.Errorf("scan %s input in %s: %w", r.format, r.sourcePath, err)
	}

	return Record{}, io.EOF
}

func (r *jsonLinesReader) Close() error {
	return closeAndNil(&r.reader)
}

func (r *jsonReader) finishArray() error {
	token, err := r.decoder.Token()
	if err != nil {
		return fmt.Errorf("close JSON array in %s: %w", r.sourcePath, err)
	}

	delim, ok := token.(json.Delim)
	if !ok || delim != ']' {
		return fmt.Errorf("expected JSON array close in %s", r.sourcePath)
	}

	r.arrayCompleted = true
	return r.ensureNoTrailingContent()
}

func (r *jsonReader) ensureNoTrailingContent() error {
	if _, err := r.decoder.Token(); err != nil {
		if err == io.EOF {
			return nil
		}
		return fmt.Errorf("verify trailing JSON content in %s: %w", r.sourcePath, err)
	}

	return fmt.Errorf("unexpected trailing JSON content in %s", r.sourcePath)
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

func closeAndNil(target *io.ReadCloser) error {
	if target == nil || *target == nil {
		return nil
	}
	err := (*target).Close()
	*target = nil
	if err != nil {
		return fmt.Errorf("close reader: %w", err)
	}
	return nil
}

func normalizeScannerBufferSizes(bufferSize, maxBufferSize int) (int, int) {
	initial := defaultScanBuffer
	if bufferSize > 0 {
		initial = bufferSize
	}

	maxCapacity := maxScanBuffer
	if maxBufferSize > 0 {
		maxCapacity = maxBufferSize
	}
	if maxCapacity < initial {
		maxCapacity = initial
	}

	return initial, maxCapacity
}
