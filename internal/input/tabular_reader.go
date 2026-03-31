package input

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/xuri/excelize/v2"
)

type delimitedReader struct {
	sourcePath  string
	reader      io.ReadCloser
	csvReader   *csv.Reader
	format      Format
	headers     []string
	lineNumber  int
	recordIndex int
	exhausted   bool
}

func newDelimitedReader(sourcePath string, reader io.ReadCloser, format Format, delimiter rune, _, _ int) (Reader, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader cannot be nil")
	}

	csvReader := csv.NewReader(reader)
	csvReader.Comma = delimiter
	csvReader.FieldsPerRecord = -1

	headers, err := csvReader.Read()
	if err == io.EOF {
		return &delimitedReader{
			sourcePath: sourcePath,
			reader:     reader,
			csvReader:  csvReader,
			format:     format,
			headers:    nil,
			lineNumber: 1,
			exhausted:  true,
		}, nil
	}
	if err != nil {
		safeClose(reader)
		return nil, fmt.Errorf("read %s header row in %s: %w", format, sourcePath, err)
	}

	return &delimitedReader{
		sourcePath: sourcePath,
		reader:     reader,
		csvReader:  csvReader,
		format:     format,
		headers:    normalizeTabularHeaders(headers),
		lineNumber: 1,
	}, nil
}

func (r *delimitedReader) Format() Format {
	return r.format
}

func (r *delimitedReader) Layout() Layout {
	return LayoutStream
}

func (r *delimitedReader) Next(ctx context.Context) (Record, error) {
	if r.exhausted {
		return Record{}, io.EOF
	}

	for {
		if err := ctx.Err(); err != nil {
			return Record{}, fmt.Errorf("read %s input %s: %w", r.format, r.sourcePath, err)
		}

		rowValues, err := r.csvReader.Read()
		if err == io.EOF {
			r.exhausted = true
			return Record{}, io.EOF
		}
		if err != nil {
			return Record{}, fmt.Errorf("read %s row %d in %s: %w", r.format, r.lineNumber+1, r.sourcePath, err)
		}

		if len(rowValues) > 0 {
			if line, _ := r.csvReader.FieldPos(0); line > 0 {
				r.lineNumber = line
			} else {
				r.lineNumber++
			}
		} else {
			r.lineNumber++
		}
		row := buildTabularRow(r.headers, rowValues)
		if !tabularRowHasValues(row) {
			continue
		}

		record := Record{
			SourcePath:  r.sourcePath,
			RecordIndex: r.recordIndex,
			LineNumber:  r.lineNumber,
			Data:        row,
		}
		r.recordIndex++
		return record, nil
	}
}

func (r *delimitedReader) Close() error {
	return closeAndNil(&r.reader)
}

type spreadsheetReader struct {
	sourcePath  string
	reader      io.ReadCloser
	workbook    *excelize.File
	rows        *excelize.Rows
	format      Format
	headers     []string
	lineNumber  int
	recordIndex int
}

func newSpreadsheetReader(sourcePath string, reader io.ReadCloser, sheet string) (Reader, error) {
	if reader == nil {
		return nil, fmt.Errorf("reader cannot be nil")
	}

	workbook, err := excelize.OpenReader(reader)
	if err != nil {
		safeClose(reader)
		return nil, fmt.Errorf("open workbook in %s: %w", sourcePath, err)
	}

	targetSheet := strings.TrimSpace(sheet)
	if targetSheet == "" {
		sheets := workbook.GetSheetList()
		if len(sheets) == 0 {
			_ = workbook.Close()
			safeClose(reader)
			return nil, fmt.Errorf("workbook %s does not contain any sheets", sourcePath)
		}
		targetSheet = sheets[0]
	}

	rows, err := workbook.Rows(targetSheet)
	if err != nil {
		_ = workbook.Close()
		safeClose(reader)
		return nil, fmt.Errorf("open sheet %q in %s: %w", targetSheet, sourcePath, err)
	}

	return &spreadsheetReader{
		sourcePath: sourcePath,
		reader:     reader,
		workbook:   workbook,
		rows:       rows,
		format:     FormatXLSX,
	}, nil
}

func (r *spreadsheetReader) Format() Format {
	return r.format
}

func (r *spreadsheetReader) Layout() Layout {
	return LayoutStream
}

func (r *spreadsheetReader) Next(ctx context.Context) (Record, error) {
	for r.rows.Next() {
		if err := ctx.Err(); err != nil {
			return Record{}, fmt.Errorf("read spreadsheet %s: %w", r.sourcePath, err)
		}

		r.lineNumber++
		columns, err := r.rows.Columns()
		if err != nil {
			return Record{}, fmt.Errorf("read spreadsheet row %d in %s: %w", r.lineNumber, r.sourcePath, err)
		}

		if r.headers == nil {
			r.headers = normalizeTabularHeaders(columns)
			continue
		}

		row := buildTabularRow(r.headers, columns)
		if !tabularRowHasValues(row) {
			continue
		}

		record := Record{
			SourcePath:  r.sourcePath,
			RecordIndex: r.recordIndex,
			LineNumber:  r.lineNumber,
			Data:        row,
		}
		r.recordIndex++
		return record, nil
	}

	if err := r.rows.Error(); err != nil {
		return Record{}, fmt.Errorf("scan spreadsheet rows in %s: %w", r.sourcePath, err)
	}

	return Record{}, io.EOF
}

func (r *spreadsheetReader) Close() error {
	var errs []error
	if r.rows != nil {
		errs = appendIfError(errs, r.rows.Close())
		r.rows = nil
	}
	if r.workbook != nil {
		errs = appendIfError(errs, r.workbook.Close())
		r.workbook = nil
	}
	if err := closeAndNil(&r.reader); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("close spreadsheet reader: %w", errors.Join(errs...))
	}
	return nil
}

func normalizeTabularHeaders(headers []string) []string {
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

func buildTabularRow(headers, values []string) map[string]any {
	row := make(map[string]any, len(headers))
	for index, header := range headers {
		value := ""
		if index < len(values) {
			value = values[index]
		}
		row[header] = value
	}
	return row
}

func tabularRowHasValues(row map[string]any) bool {
	for _, value := range row {
		text, ok := value.(string)
		if ok && strings.TrimSpace(text) != "" {
			return true
		}
	}
	return false
}

func appendIfError(errs []error, err error) []error {
	if err == nil {
		return errs
	}
	return append(errs, err)
}
