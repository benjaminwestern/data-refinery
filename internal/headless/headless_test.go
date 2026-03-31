package headless

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunPrintsJSONReportForJSONLInput(t *testing.T) {
	dir := t.TempDir()
	logDir := filepath.Join(dir, "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("failed to create log dir: %v", err)
	}

	filePath := filepath.Join(dir, "records.jsonl")
	content := strings.Join([]string{
		`{"id":"1","name":"alpha"}`,
		`{"id":"2","name":"beta"}`,
		`{"id":"1","name":"alpha"}`,
	}, "\n") + "\n"
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write fixture: %v", err)
	}

	stdout := captureStdout(t, func() {
		Run(context.Background(), &Config{
			Paths:               filePath,
			Key:                 "id",
			Workers:             1,
			LogPath:             logDir,
			OutputFormat:        "json",
			CheckKey:            true,
			CheckRow:            true,
			ShowFolderBreakdown: false,
			EnableTxtOutput:     true,
			EnableJSONOutput:    true,
		})
	})

	if !strings.Contains(stdout, "Running in headless mode...") {
		t.Fatalf("expected headless startup message, got: %s", stdout)
	}
	if !strings.Contains(stdout, "\"totalRowsProcessed\": 3") {
		t.Fatalf("expected JSON report output, got: %s", stdout)
	}
	if !strings.Contains(stdout, "\"duplicateIds\"") {
		t.Fatalf("expected duplicate ID data in output, got: %s", stdout)
	}
}

func TestRunPrintsJSONReportForCSVInput(t *testing.T) {
	dir := t.TempDir()
	logDir := filepath.Join(dir, "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("failed to create log dir: %v", err)
	}

	filePath := filepath.Join(dir, "records.csv")
	content := strings.Join([]string{
		"id,name",
		"1,alpha",
		"2,beta",
		"1,alpha",
	}, "\n") + "\n"
	if err := os.WriteFile(filePath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write fixture: %v", err)
	}

	stdout := captureStdout(t, func() {
		Run(context.Background(), &Config{
			Paths:               filePath,
			Key:                 "id",
			Workers:             1,
			LogPath:             logDir,
			OutputFormat:        "json",
			CheckKey:            true,
			CheckRow:            true,
			ShowFolderBreakdown: false,
			EnableTxtOutput:     true,
			EnableJSONOutput:    true,
		})
	})

	if !strings.Contains(stdout, "\"totalRowsProcessed\": 3") {
		t.Fatalf("expected JSON report output, got: %s", stdout)
	}
	if !strings.Contains(stdout, "\"duplicateIds\"") {
		t.Fatalf("expected duplicate ID data in output, got: %s", stdout)
	}
}

func TestRunPrintsJSONReportForXMLInput(t *testing.T) {
	dir := t.TempDir()
	logDir := filepath.Join(dir, "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("failed to create log dir: %v", err)
	}

	firstPath := filepath.Join(dir, "first.xml")
	secondPath := filepath.Join(dir, "second.xml")
	firstXML := strings.TrimSpace(`
		<customer>
			<id>xml-1</id>
			<name>alpha</name>
		</customer>
	`)
	secondXML := strings.TrimSpace(`
		<customer>
			<id>xml-1</id>
			<name>beta</name>
		</customer>
	`)
	if err := os.WriteFile(firstPath, []byte(firstXML), 0o644); err != nil {
		t.Fatalf("failed to write first XML fixture: %v", err)
	}
	if err := os.WriteFile(secondPath, []byte(secondXML), 0o644); err != nil {
		t.Fatalf("failed to write second XML fixture: %v", err)
	}

	stdout := captureStdout(t, func() {
		Run(context.Background(), &Config{
			Paths:               dir,
			Key:                 "id",
			Workers:             1,
			LogPath:             logDir,
			OutputFormat:        "json",
			CheckKey:            true,
			CheckRow:            true,
			ShowFolderBreakdown: false,
			EnableTxtOutput:     true,
			EnableJSONOutput:    true,
		})
	})

	if !strings.Contains(stdout, "\"totalRowsProcessed\": 2") {
		t.Fatalf("expected JSON report output, got: %s", stdout)
	}
	if !strings.Contains(stdout, "\"duplicateIds\"") {
		t.Fatalf("expected duplicate ID data in output, got: %s", stdout)
	}
}

func TestRunPrintsJSONReportForXMLRecordPathInput(t *testing.T) {
	dir := t.TempDir()
	logDir := filepath.Join(dir, "logs")
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		t.Fatalf("failed to create log dir: %v", err)
	}

	path := filepath.Join(dir, "customers.xml")
	xmlBody := strings.Join([]string{
		"<customers>",
		"  <customer>",
		"    <id>xml-1</id>",
		"    <name>alpha</name>",
		"  </customer>",
		"  <customer>",
		"    <id>xml-1</id>",
		"    <name>beta</name>",
		"  </customer>",
		"  <customer>",
		"    <id>xml-2</id>",
		"    <name>gamma</name>",
		"  </customer>",
		"</customers>",
	}, "\n")
	if err := os.WriteFile(path, []byte(xmlBody), 0o644); err != nil {
		t.Fatalf("failed to write XML fixture: %v", err)
	}

	stdout := captureStdout(t, func() {
		Run(context.Background(), &Config{
			Paths:               path,
			Key:                 "id",
			XMLRecordPath:       "customers.customer",
			Workers:             1,
			LogPath:             logDir,
			OutputFormat:        "json",
			CheckKey:            true,
			CheckRow:            true,
			ShowFolderBreakdown: false,
			EnableTxtOutput:     true,
			EnableJSONOutput:    true,
		})
	})

	if !strings.Contains(stdout, "\"totalRowsProcessed\": 3") {
		t.Fatalf("expected JSON report output, got: %s", stdout)
	}
	if !strings.Contains(stdout, "\"duplicateIds\"") {
		t.Fatalf("expected duplicate ID data in output, got: %s", stdout)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	originalStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}

	os.Stdout = writer
	done := make(chan string, 1)

	go func() {
		var buf bytes.Buffer
		_, _ = io.Copy(&buf, reader)
		done <- buf.String()
	}()

	fn()

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}
	os.Stdout = originalStdout

	output := <-done
	if err := reader.Close(); err != nil {
		t.Fatalf("failed to close reader: %v", err)
	}

	return output
}
