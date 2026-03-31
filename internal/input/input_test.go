package input

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
)

type stubSource struct {
	path string
	body string
}

func (s stubSource) Path() string { return s.path }

func (s stubSource) Open(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.body)), nil
}

type fileSource struct {
	path string
}

func (f fileSource) Path() string { return f.path }

func (f fileSource) Open(context.Context) (io.ReadCloser, error) {
	return os.Open(f.path)
}

func TestDetectFormatFromPath(t *testing.T) {
	t.Parallel()

	cases := map[string]Format{
		"/tmp/data.csv":               FormatCSV,
		"/tmp/data.tsv":               FormatTSV,
		"/tmp/data.xlsx":              FormatXLSX,
		"/tmp/data.xml":               FormatXML,
		"/tmp/data.json":              FormatJSON,
		"/tmp/data.ndjson":            FormatNDJSON,
		"/tmp/data.jsonl":             FormatJSONL,
		"gs://bucket/data.CSV":        FormatCSV,
		"gs://bucket/data.JSONL":      FormatJSONL,
		"gs://bucket/data.unknown":    FormatUnknown,
		"/tmp/data.with.multiple.ext": FormatUnknown,
	}

	for path, expected := range cases {
		path := path
		expected := expected
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			if got := DetectFormatFromPath(path); got != expected {
				t.Fatalf("DetectFormatFromPath(%q) = %q, want %q", path, got, expected)
			}
		})
	}
}

func TestReadAllJSONObject(t *testing.T) {
	t.Parallel()

	src := stubSource{
		path: "/tmp/object.json",
		body: `{"id":"1","name":"Alice","nested":{"active":true}}`,
	}

	records, err := ReadAll(context.Background(), src)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	record := records[0]
	if record.SourcePath != src.path {
		t.Fatalf("record.SourcePath = %q, want %q", record.SourcePath, src.path)
	}
	if record.RecordIndex != 0 {
		t.Fatalf("record.RecordIndex = %d, want 0", record.RecordIndex)
	}
	if got := record.Data["id"]; got != "1" {
		t.Fatalf("record.Data[\"id\"] = %v, want %q", got, "1")
	}
}

func TestReadAllJSONArray(t *testing.T) {
	t.Parallel()

	src := stubSource{
		path: "gs://bucket/array.json",
		body: `[
			{"id":"1","name":"Alice"},
			{"id":"2","name":"Bob"}
		]`,
	}

	reader, err := NewReader(context.Background(), src)
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Fatalf("reader.Close failed: %v", err)
		}
	}()

	if got := reader.Format(); got != FormatJSON {
		t.Fatalf("reader.Format() = %q, want %q", got, FormatJSON)
	}
	if got := reader.Layout(); got != LayoutArray {
		t.Fatalf("reader.Layout() = %q, want %q", got, LayoutArray)
	}

	first, err := reader.Next(context.Background())
	if err != nil {
		t.Fatalf("first Next failed: %v", err)
	}
	if first.RecordIndex != 0 {
		t.Fatalf("first.RecordIndex = %d, want 0", first.RecordIndex)
	}

	second, err := reader.Next(context.Background())
	if err != nil {
		t.Fatalf("second Next failed: %v", err)
	}
	if second.RecordIndex != 1 {
		t.Fatalf("second.RecordIndex = %d, want 1", second.RecordIndex)
	}

	_, err = reader.Next(context.Background())
	if err != io.EOF {
		t.Fatalf("third Next error = %v, want io.EOF", err)
	}
}

func TestNewReaderPreservesMalformedJSONFamilyLines(t *testing.T) {
	t.Parallel()

	reader, err := NewReader(context.Background(), stubSource{
		path: "/tmp/workflow.json",
		body: strings.Join([]string{
			`{"id":"1"}`,
			`not-json`,
			`{"id":"2"}`,
		}, "\n"),
	}, WithJSONMode(JSONModeLineStream), WithDecodeErrorsAsRecords())
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Fatalf("reader.Close failed: %v", err)
		}
	}()

	if got := reader.Layout(); got != LayoutStream {
		t.Fatalf("reader.Layout() = %q, want %q", got, LayoutStream)
	}

	first, err := reader.Next(context.Background())
	if err != nil {
		t.Fatalf("first Next failed: %v", err)
	}
	if first.DecodeErr != nil {
		t.Fatalf("expected first record to decode successfully, got %v", first.DecodeErr)
	}
	if got := first.LineNumber; got != 1 {
		t.Fatalf("first.LineNumber = %d, want 1", got)
	}

	second, err := reader.Next(context.Background())
	if err != nil {
		t.Fatalf("second Next failed: %v", err)
	}
	if second.DecodeErr == nil {
		t.Fatal("expected malformed line to be preserved with a decode error")
	}
	if got := string(second.Raw); got != "not-json" {
		t.Fatalf("second.Raw = %q, want %q", got, "not-json")
	}
	if got := second.LineNumber; got != 2 {
		t.Fatalf("second.LineNumber = %d, want 2", got)
	}

	third, err := reader.Next(context.Background())
	if err != nil {
		t.Fatalf("third Next failed: %v", err)
	}
	if third.DecodeErr != nil {
		t.Fatalf("expected third record to decode successfully, got %v", third.DecodeErr)
	}
	if got := third.RecordIndex; got != 2 {
		t.Fatalf("third.RecordIndex = %d, want 2", got)
	}
}

func TestReadAllNDJSONAndJSONL(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		path       string
		body       string
		wantFormat Format
	}{
		{
			name:       "ndjson",
			path:       "/tmp/data.ndjson",
			body:       "{\"id\":\"1\"}\n\n{\"id\":\"2\"}\n",
			wantFormat: FormatNDJSON,
		},
		{
			name:       "jsonl",
			path:       "/tmp/data.jsonl",
			body:       "{\"id\":\"3\"}\n{\"id\":\"4\"}\n",
			wantFormat: FormatJSONL,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			reader, err := NewReader(context.Background(), stubSource{path: tc.path, body: tc.body})
			if err != nil {
				t.Fatalf("NewReader failed: %v", err)
			}
			defer func() {
				if err := reader.Close(); err != nil {
					t.Fatalf("reader.Close failed: %v", err)
				}
			}()

			if got := reader.Format(); got != tc.wantFormat {
				t.Fatalf("reader.Format() = %q, want %q", got, tc.wantFormat)
			}

			var records []Record
			for {
				record, err := reader.Next(context.Background())
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("Next failed: %v", err)
				}
				records = append(records, record)
			}

			if len(records) != 2 {
				t.Fatalf("expected 2 records, got %d", len(records))
			}
			if records[0].RecordIndex != 0 || records[1].RecordIndex != 1 {
				t.Fatalf("unexpected record indexes: %#v", records)
			}
		})
	}
}

func TestReadAllXML(t *testing.T) {
	t.Parallel()

	src := stubSource{
		path: "/tmp/customer.xml",
		body: strings.TrimSpace(`
			<customer status="active">
				<id>1</id>
				<name>Alice</name>
				<emails>
					<email primary="true">alice@example.com</email>
					<email>ops@example.com</email>
				</emails>
				<tags>
					<tag>vip</tag>
					<tag>gold</tag>
				</tags>
			</customer>
		`),
	}

	records, err := ReadAll(context.Background(), src)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}

	record := records[0]
	if got := record.SourcePath; got != src.path {
		t.Fatalf("record.SourcePath = %q, want %q", got, src.path)
	}
	if got := record.LineNumber; got != 1 {
		t.Fatalf("record.LineNumber = %d, want 1", got)
	}
	if got := record.Data["@status"]; got != "active" {
		t.Fatalf("record.Data[@status] = %v, want active", got)
	}
	if got := record.Data["id"]; got != "1" {
		t.Fatalf("record.Data[id] = %v, want 1", got)
	}

	emails, ok := record.Data["emails"].(map[string]any)
	if !ok {
		t.Fatalf("record.Data[emails] has unexpected type %T", record.Data["emails"])
	}
	emailList, ok := emails["email"].([]any)
	if !ok || len(emailList) != 2 {
		t.Fatalf("emails[email] = %#v, want 2 entries", emails["email"])
	}
	firstEmail, ok := emailList[0].(map[string]any)
	if !ok {
		t.Fatalf("first email has unexpected type %T", emailList[0])
	}
	if got := firstEmail["@primary"]; got != "true" {
		t.Fatalf("first email @primary = %v, want true", got)
	}
	if got := firstEmail["#text"]; got != "alice@example.com" {
		t.Fatalf("first email #text = %v, want alice@example.com", got)
	}

	tags, ok := record.Data["tags"].(map[string]any)
	if !ok {
		t.Fatalf("record.Data[tags] has unexpected type %T", record.Data["tags"])
	}
	tagList, ok := tags["tag"].([]any)
	if !ok || len(tagList) != 2 {
		t.Fatalf("tags[tag] = %#v, want 2 entries", tags["tag"])
	}
	if tagList[0] != "vip" || tagList[1] != "gold" {
		t.Fatalf("unexpected tag values: %#v", tagList)
	}
}

func TestReadAllXMLRecordPath(t *testing.T) {
	t.Parallel()

	src := stubSource{
		path: "/tmp/customers.xml",
		body: strings.Join([]string{
			"<customers>",
			"  <customer status=\"active\">",
			"    <id>1</id>",
			"    <name>Alice</name>",
			"  </customer>",
			"  <customer status=\"pending\">",
			"    <id>2</id>",
			"    <name>Bob</name>",
			"  </customer>",
			"</customers>",
		}, "\n"),
	}

	reader, err := NewReader(context.Background(), src, WithXMLRecordPath("customers.customer"))
	if err != nil {
		t.Fatalf("NewReader failed: %v", err)
	}
	defer func() {
		if err := reader.Close(); err != nil {
			t.Fatalf("reader.Close failed: %v", err)
		}
	}()

	if got := reader.Layout(); got != LayoutStream {
		t.Fatalf("reader.Layout() = %q, want %q", got, LayoutStream)
	}

	first, err := reader.Next(context.Background())
	if err != nil {
		t.Fatalf("first Next failed: %v", err)
	}
	if got := first.LineNumber; got != 2 {
		t.Fatalf("first.LineNumber = %d, want 2", got)
	}
	if got := first.Data["@status"]; got != "active" {
		t.Fatalf("first.Data[@status] = %v, want active", got)
	}
	if got := first.Data["id"]; got != "1" {
		t.Fatalf("first.Data[id] = %v, want 1", got)
	}

	second, err := reader.Next(context.Background())
	if err != nil {
		t.Fatalf("second Next failed: %v", err)
	}
	if got := second.LineNumber; got != 6 {
		t.Fatalf("second.LineNumber = %d, want 6", got)
	}
	if got := second.Data["@status"]; got != "pending" {
		t.Fatalf("second.Data[@status] = %v, want pending", got)
	}
	if got := second.Data["name"]; got != "Bob" {
		t.Fatalf("second.Data[name] = %v, want Bob", got)
	}

	_, err = reader.Next(context.Background())
	if err != io.EOF {
		t.Fatalf("third Next error = %v, want io.EOF", err)
	}
}

func TestNewReaderRejectsUnmatchedXMLRecordPath(t *testing.T) {
	t.Parallel()

	_, err := NewReader(context.Background(), stubSource{
		path: "/tmp/customers.xml",
		body: `<customers><customer><id>1</id></customer></customers>`,
	}, WithXMLRecordPath("customers.account"))
	if err == nil {
		t.Fatal("expected unmatched XML record path to fail")
	}
	if !strings.Contains(err.Error(), `XML record path "customers.account" did not match any elements`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewReaderRejectsUnknownFormat(t *testing.T) {
	t.Parallel()

	_, err := NewReader(context.Background(), stubSource{path: "/tmp/data.txt", body: `{"id":"1"}`})
	if err == nil {
		t.Fatal("expected error for unsupported input format")
	}
}

func TestTabularReaderContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		ext      string
		wantRows []map[string]any
		wantLine []int
	}{
		{
			name: "csv",
			ext:  ".csv",
			wantRows: []map[string]any{
				{"id": "1", "name": "Alice"},
				{"id": "2", "name": "Bob"},
			},
			wantLine: []int{2, 4},
		},
		{
			name: "tsv",
			ext:  ".tsv",
			wantRows: []map[string]any{
				{"id": "3", "name": "Cara"},
				{"id": "4", "name": "Drew"},
			},
			wantLine: []int{2, 4},
		},
		{
			name: "xlsx",
			ext:  ".xlsx",
			wantRows: []map[string]any{
				{"id": "5", "name": "Eve"},
				{"id": "6", "name": "Finn"},
			},
			wantLine: []int{2, 3},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			path := filepath.Join(dir, "records"+tc.ext)
			switch tc.ext {
			case ".csv":
				content := strings.Join([]string{
					"id,name",
					"1,Alice",
					"",
					"2,Bob",
				}, "\n") + "\n"
				if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
					t.Fatalf("failed to write CSV fixture: %v", err)
				}
			case ".tsv":
				content := strings.Join([]string{
					"id\tname",
					"3\tCara",
					"",
					"4\tDrew",
				}, "\n") + "\n"
				if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
					t.Fatalf("failed to write TSV fixture: %v", err)
				}
			case ".xlsx":
				workbook := excelize.NewFile()
				sheet := workbook.GetSheetName(workbook.GetActiveSheetIndex())
				if err := workbook.SetSheetRow(sheet, "A1", &[]any{"id", "name"}); err != nil {
					t.Fatalf("failed to write XLSX header: %v", err)
				}
				if err := workbook.SetSheetRow(sheet, "A2", &[]any{"5", "Eve"}); err != nil {
					t.Fatalf("failed to write XLSX row 1: %v", err)
				}
				if err := workbook.SetSheetRow(sheet, "A3", &[]any{"6", "Finn"}); err != nil {
					t.Fatalf("failed to write XLSX row 2: %v", err)
				}
				if err := workbook.SaveAs(path); err != nil {
					t.Fatalf("failed to save XLSX fixture: %v", err)
				}
			}

			reader, err := NewReader(context.Background(), fileSource{path: path})
			if err != nil {
				t.Fatalf("NewReader failed: %v", err)
			}
			defer func() {
				if err := reader.Close(); err != nil {
					t.Fatalf("reader.Close failed: %v", err)
				}
			}()

			var records []Record
			for {
				record, err := reader.Next(context.Background())
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("Next failed: %v", err)
				}
				records = append(records, record)
			}

			if len(records) != len(tc.wantRows) {
				t.Fatalf("expected %d records, got %d", len(tc.wantRows), len(records))
			}

			for i, want := range tc.wantRows {
				got := records[i]
				if got.RecordIndex != i {
					t.Fatalf("record %d RecordIndex = %d, want %d", i, got.RecordIndex, i)
				}
				if got.LineNumber != tc.wantLine[i] {
					t.Fatalf("record %d LineNumber = %d, want %d", i, got.LineNumber, tc.wantLine[i])
				}
				for key, wantValue := range want {
					if gotValue := got.Data[key]; gotValue != wantValue {
						t.Fatalf("record %d data[%q] = %v, want %v", i, key, gotValue, wantValue)
					}
				}
			}
		})
	}
}
