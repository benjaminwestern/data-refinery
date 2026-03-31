package ingest

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xuri/excelize/v2"
)

func TestLoadConfigFileResolvesRelativePaths(t *testing.T) {
	root := t.TempDir()
	configDir := filepath.Join(root, "configs")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatalf("failed to create config dir: %v", err)
	}

	configPath := filepath.Join(configDir, "ingest.json")
	content := `{
  "paths": ["../input/customers.csv", "gs://example-bucket/customers.jsonl"],
  "logPath": "../logs",
  "outputPath": "../out/unified.ndjson",
  "mappingFile": "../mappings/legacy.yaml",
  "statsOutputPath": "../reports/summary.json"
}`
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	cfg, err := LoadConfigFile(configPath)
	if err != nil {
		t.Fatalf("LoadConfigFile returned error: %v", err)
	}

	if got, want := cfg.Paths[0], filepath.Join(root, "input", "customers.csv"); got != want {
		t.Fatalf("expected resolved local input path %q, got %q", want, got)
	}
	if got, want := cfg.Paths[1], "gs://example-bucket/customers.jsonl"; got != want {
		t.Fatalf("expected remote input path %q, got %q", want, got)
	}
	if got, want := cfg.LogPath, filepath.Join(root, "logs"); got != want {
		t.Fatalf("expected log path %q, got %q", want, got)
	}
	if got, want := cfg.OutputPath, filepath.Join(root, "out", "unified.ndjson"); got != want {
		t.Fatalf("expected output path %q, got %q", want, got)
	}
	if got, want := cfg.MappingFile, filepath.Join(root, "mappings", "legacy.yaml"); got != want {
		t.Fatalf("expected mapping file %q, got %q", want, got)
	}
	if got, want := cfg.StatsOutputPath, filepath.Join(root, "reports", "summary.json"); got != want {
		t.Fatalf("expected stats output path %q, got %q", want, got)
	}
}

func TestLoadMappingSetSupportsLegacyFormat(t *testing.T) {
	root := t.TempDir()
	mappingPath := filepath.Join(root, "legacy.yaml")
	content := `
customers.csv:
  columns:
    customer_id: Id
    email: Email
  attributes:
    tier: CustomerTier
  data_source: legacy-import
`
	if err := os.WriteFile(mappingPath, []byte(strings.TrimSpace(content)), 0o644); err != nil {
		t.Fatalf("failed to write mapping file: %v", err)
	}

	mappings, err := LoadMappingSet(mappingPath)
	if err != nil {
		t.Fatalf("LoadMappingSet returned error: %v", err)
	}
	if len(mappings.Files) != 1 {
		t.Fatalf("expected 1 mapping, got %d", len(mappings.Files))
	}
	if got, want := mappings.Files[0].Match.Name, "customers.csv"; got != want {
		t.Fatalf("expected legacy mapping to match %q, got %q", want, got)
	}

	resolved := mappings.Resolve("/tmp/customers.csv", "csv")
	if resolved == nil {
		t.Fatal("expected legacy mapping to resolve")
	}
	if got, want := resolved.EffectiveDataSource, "legacy-import"; got != want {
		t.Fatalf("expected legacy data source %q, got %q", want, got)
	}
}

func TestRunNormalizesCSVAndJSONLInputs(t *testing.T) {
	root := t.TempDir()
	inputDir := filepath.Join(root, "input")
	logDir := filepath.Join(root, "logs")
	outputPath := filepath.Join(root, "out", "unified.ndjson")
	mappingPath := filepath.Join(root, "mappings.yaml")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("failed to create input dir: %v", err)
	}

	csvContent := "customer_id,first_name,email,status\n1,Alice,alice@example.com,active\n2,Bob,bob@example.com,inactive\n"
	if err := os.WriteFile(filepath.Join(inputDir, "customers.csv"), []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write csv fixture: %v", err)
	}

	jsonlContent := `{"identifier":"99","profile":{"email":"json@example.com"},"phone":"0400123123","source":"api","notes":"vip"}` + "\n"
	if err := os.WriteFile(filepath.Join(inputDir, "customers.jsonl"), []byte(jsonlContent), 0o644); err != nil {
		t.Fatalf("failed to write jsonl fixture: %v", err)
	}

	mappingContent := `
defaults:
  trimWhitespace: true
files:
  - match:
      name: customers.csv
      format: csv
    columns:
      customer_id: Id
      first_name: FirstName
      email: Email
    attributes:
      status: AccountStatus
    dataSource: csv-import
  - match:
      glob: "*.jsonl"
      format: jsonl
    columns:
      identifier: Id
      profile.email: Email
      phone: Mobile
      source: DataSource
    includeUnmappedAsAttributes: true
`
	if err := os.WriteFile(mappingPath, []byte(strings.TrimSpace(mappingContent)), 0o644); err != nil {
		t.Fatalf("failed to write mapping file: %v", err)
	}

	summary, err := Run(context.Background(), &Config{
		Paths:           []string{inputDir},
		Workers:         1,
		LogPath:         logDir,
		OutputPath:      outputPath,
		MappingFile:     mappingPath,
		RequireMappings: true,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	if got, want := summary.RowsWritten, int64(3); got != want {
		t.Fatalf("expected %d rows written, got %d", want, got)
	}
	if got, want := summary.FilesProcessed, 2; got != want {
		t.Fatalf("expected %d files processed, got %d", want, got)
	}
	if summary.StatsOutputPath == "" {
		t.Fatal("expected stats output path to be populated")
	}

	records := readUnifiedRecords(t, outputPath)
	if len(records) != 3 {
		t.Fatalf("expected 3 unified output records, got %d", len(records))
	}

	var csvRecordFound bool
	var jsonRecord *unifiedRecord
	for index := range records {
		record := &records[index]
		switch record.SourceFile {
		case "customers.csv":
			if record.FirstName != nil && *record.FirstName == "Alice" {
				csvRecordFound = true
				if record.DataSource == nil || *record.DataSource != "csv-import" {
					t.Fatalf("expected CSV data source to be injected, got %+v", record.DataSource)
				}
				if len(record.Attributes) == 0 || record.Attributes[0].Key != "AccountStatus" {
					t.Fatalf("expected CSV attribute mapping to be preserved, got %+v", record.Attributes)
				}
			}
		case "customers.jsonl":
			jsonRecord = record
		}
		if record.BQInsertedDate == "" {
			t.Fatalf("expected record %d to include an ingest timestamp", index)
		}
	}

	if !csvRecordFound {
		t.Fatal("expected a normalized CSV record for Alice")
	}

	if jsonRecord == nil {
		t.Fatal("expected a normalized JSONL record")
	}
	if jsonRecord.Email == nil || *jsonRecord.Email != "json@example.com" {
		t.Fatalf("expected nested JSON path lookup to populate Email, got %+v", jsonRecord.Email)
	}
	if jsonRecord.DataSource == nil || *jsonRecord.DataSource != "api" {
		t.Fatalf("expected JSON data source to come from the source field, got %+v", jsonRecord.DataSource)
	}
	if !hasAttribute(jsonRecord.Attributes, "notes", "vip") {
		t.Fatalf("expected unmapped JSON keys to become attributes, got %+v", jsonRecord.Attributes)
	}
	if hasAttributeKey(jsonRecord.Attributes, "profile") {
		t.Fatalf("did not expect consumed top-level JSON objects to be emitted as attributes, got %+v", jsonRecord.Attributes)
	}
}

func TestRunSupportsXLSXInput(t *testing.T) {
	root := t.TempDir()
	logDir := filepath.Join(root, "logs")
	outputPath := filepath.Join(root, "out", "spreadsheet.ndjson")
	inputPath := filepath.Join(root, "customers.xlsx")
	mappingPath := filepath.Join(root, "mappings.yaml")

	workbook := excelize.NewFile()
	sheetName := workbook.GetSheetName(0)
	rows := [][]string{
		{"contact_id", "email_address", "tier"},
		{"501", "spreadsheet@example.com", "gold"},
	}
	for rowIndex, row := range rows {
		cell, err := excelize.CoordinatesToCellName(1, rowIndex+1)
		if err != nil {
			t.Fatalf("failed to compute spreadsheet cell: %v", err)
		}
		interfaceRow := make([]interface{}, len(row))
		for index, value := range row {
			interfaceRow[index] = value
		}
		if err := workbook.SetSheetRow(sheetName, cell, &interfaceRow); err != nil {
			t.Fatalf("failed to write spreadsheet row: %v", err)
		}
	}
	if err := workbook.SaveAs(inputPath); err != nil {
		t.Fatalf("failed to save spreadsheet fixture: %v", err)
	}

	mappingContent := `
customers.xlsx:
  columns:
    contact_id: Id
    email_address: Email
  attributes:
    tier: CustomerTier
  data_source: spreadsheet-import
`
	if err := os.WriteFile(mappingPath, []byte(strings.TrimSpace(mappingContent)), 0o644); err != nil {
		t.Fatalf("failed to write legacy spreadsheet mapping: %v", err)
	}

	summary, err := Run(context.Background(), &Config{
		Paths:           []string{inputPath},
		Workers:         1,
		LogPath:         logDir,
		OutputPath:      outputPath,
		MappingFile:     mappingPath,
		RequireMappings: true,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got, want := summary.RowsWritten, int64(1); got != want {
		t.Fatalf("expected %d row written, got %d", want, got)
	}

	records := readUnifiedRecords(t, outputPath)
	if len(records) != 1 {
		t.Fatalf("expected 1 unified output record, got %d", len(records))
	}
	record := records[0]
	if record.ID == nil || *record.ID != "501" {
		t.Fatalf("expected spreadsheet Id mapping to be preserved, got %+v", record.ID)
	}
	if record.Email == nil || *record.Email != "spreadsheet@example.com" {
		t.Fatalf("expected spreadsheet Email mapping to be preserved, got %+v", record.Email)
	}
	if record.DataSource == nil || *record.DataSource != "spreadsheet-import" {
		t.Fatalf("expected spreadsheet data source to be injected, got %+v", record.DataSource)
	}
	if !hasAttribute(record.Attributes, "CustomerTier", "gold") {
		t.Fatalf("expected spreadsheet attributes to be preserved, got %+v", record.Attributes)
	}
}

func TestRunSupportsTSVAndJSONInputs(t *testing.T) {
	root := t.TempDir()
	inputDir := filepath.Join(root, "input")
	logDir := filepath.Join(root, "logs")
	outputPath := filepath.Join(root, "out", "unified.ndjson")
	mappingPath := filepath.Join(root, "mappings.yaml")
	if err := os.MkdirAll(inputDir, 0o755); err != nil {
		t.Fatalf("failed to create input dir: %v", err)
	}

	tsvContent := "customer_id\tfirst_name\temail\tstatus\n301\tTara\ttara@example.com\tactive\n"
	if err := os.WriteFile(filepath.Join(inputDir, "customers.tsv"), []byte(tsvContent), 0o644); err != nil {
		t.Fatalf("failed to write tsv fixture: %v", err)
	}

	jsonContent := `[
  {
    "identifier": "401",
    "first_name": "Uma",
    "email": "uma@example.com",
    "source": "json-import",
    "status": "trial"
  },
  {
    "identifier": "402",
    "first_name": "Victor",
    "email": "victor@example.com",
    "source": "json-import",
    "status": "active"
  }
]`
	if err := os.WriteFile(filepath.Join(inputDir, "customers.json"), []byte(jsonContent), 0o644); err != nil {
		t.Fatalf("failed to write json fixture: %v", err)
	}

	mappingContent := `
files:
  - match:
      name: customers.tsv
      format: tsv
    columns:
      customer_id: Id
      first_name: FirstName
      email: Email
    attributes:
      status: SubscriptionStatus
    dataSource: tsv-import
  - match:
      name: customers.json
      format: json
    columns:
      identifier: Id
      first_name: FirstName
      email: Email
      source: DataSource
    attributes:
      status: SubscriptionStatus
`
	if err := os.WriteFile(mappingPath, []byte(strings.TrimSpace(mappingContent)), 0o644); err != nil {
		t.Fatalf("failed to write mapping file: %v", err)
	}

	summary, err := Run(context.Background(), &Config{
		Paths:           []string{inputDir},
		Workers:         1,
		LogPath:         logDir,
		OutputPath:      outputPath,
		MappingFile:     mappingPath,
		RequireMappings: true,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got, want := summary.RowsWritten, int64(3); got != want {
		t.Fatalf("expected %d rows written, got %d", want, got)
	}

	records := readUnifiedRecords(t, outputPath)
	if len(records) != 3 {
		t.Fatalf("expected 3 unified records, got %d", len(records))
	}

	var tsvRecordFound bool
	var jsonRecordCount int
	for _, record := range records {
		switch record.SourceFile {
		case "customers.tsv":
			tsvRecordFound = true
			if record.DataSource == nil || *record.DataSource != "tsv-import" {
				t.Fatalf("expected TSV data source to be injected, got %+v", record.DataSource)
			}
			if !hasAttribute(record.Attributes, "SubscriptionStatus", "active") {
				t.Fatalf("expected TSV attribute mapping, got %+v", record.Attributes)
			}
		case "customers.json":
			jsonRecordCount++
			if record.DataSource == nil || *record.DataSource != "json-import" {
				t.Fatalf("expected JSON data source to come from the source field, got %+v", record.DataSource)
			}
			if !hasAttributeKey(record.Attributes, "SubscriptionStatus") {
				t.Fatalf("expected JSON attributes to include SubscriptionStatus, got %+v", record.Attributes)
			}
		}
	}

	if !tsvRecordFound {
		t.Fatal("expected a TSV record to be normalized")
	}
	if got, want := jsonRecordCount, 2; got != want {
		t.Fatalf("expected %d JSON records, got %d", want, got)
	}
}

func TestRunMapsNestedJSONFieldsIntoAttributesArray(t *testing.T) {
	root := t.TempDir()
	logDir := filepath.Join(root, "logs")
	outputPath := filepath.Join(root, "out", "complex.ndjson")
	inputPath := filepath.Join(root, "profiles.json")
	mappingPath := filepath.Join(root, "mappings.yaml")

	jsonContent := `[
  {
    "person": {
      "id": "9001",
      "profile": {
        "first_name": "Willow",
        "last_name": "Hart",
        "email": "willow@example.com"
      },
      "contact": {
        "mobile": "0400999000",
        "postcode": "3005"
      },
      "preferences": {
        "channel": "sms",
        "segment": "renewal"
      },
      "loyalty": {
        "tier": "gold",
        "points": 1200
      }
    },
    "metadata": {
      "source": "crm-sync",
      "created_at": "2026-01-10T09:00:00Z",
      "updated_at": "2026-02-02T18:30:00Z"
    }
  }
]`
	if err := os.WriteFile(inputPath, []byte(jsonContent), 0o644); err != nil {
		t.Fatalf("failed to write complex json fixture: %v", err)
	}

	mappingContent := `
files:
  - match:
      name: profiles.json
      format: json
    columns:
      person.id: Id
      person.profile.first_name: FirstName
      person.profile.last_name: LastName
      person.profile.email: Email
      person.contact.mobile: Mobile
      person.contact.postcode: PostCode
      metadata.source: DataSource
      metadata.created_at: SourceCreatedDate
      metadata.updated_at: SourceModifiedDate
    attributes:
      person.preferences.channel: PreferredChannel
      person.preferences.segment: CustomerSegment
      person.loyalty.tier: LoyaltyTier
      person.loyalty.points: LoyaltyPoints
`
	if err := os.WriteFile(mappingPath, []byte(strings.TrimSpace(mappingContent)), 0o644); err != nil {
		t.Fatalf("failed to write complex mapping file: %v", err)
	}

	summary, err := Run(context.Background(), &Config{
		Paths:           []string{inputPath},
		Workers:         1,
		LogPath:         logDir,
		OutputPath:      outputPath,
		MappingFile:     mappingPath,
		RequireMappings: true,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got, want := summary.RowsWritten, int64(1); got != want {
		t.Fatalf("expected %d row written, got %d", want, got)
	}

	records := readUnifiedRecords(t, outputPath)
	if len(records) != 1 {
		t.Fatalf("expected 1 unified record, got %d", len(records))
	}
	record := records[0]
	if record.DataSource == nil || *record.DataSource != "crm-sync" {
		t.Fatalf("expected nested metadata source to map into DataSource, got %+v", record.DataSource)
	}
	if !hasAttribute(record.Attributes, "PreferredChannel", "sms") {
		t.Fatalf("expected nested preference to be standardized into Attributes, got %+v", record.Attributes)
	}
	if !hasAttribute(record.Attributes, "CustomerSegment", "renewal") {
		t.Fatalf("expected nested segment to be standardized into Attributes, got %+v", record.Attributes)
	}
	if !hasAttribute(record.Attributes, "LoyaltyTier", "gold") {
		t.Fatalf("expected nested loyalty tier to be standardized into Attributes, got %+v", record.Attributes)
	}
	if !hasAttribute(record.Attributes, "LoyaltyPoints", "1200") {
		t.Fatalf("expected nested loyalty points to be standardized into Attributes, got %+v", record.Attributes)
	}
}

func TestRunSupportsXMLInput(t *testing.T) {
	root := t.TempDir()
	logDir := filepath.Join(root, "logs")
	outputPath := filepath.Join(root, "out", "customers.ndjson")
	inputPath := filepath.Join(root, "customers.xml")
	mappingPath := filepath.Join(root, "mappings.yaml")

	xmlContent := `<?xml version="1.0" encoding="UTF-8"?>
<contacts>
  <contact>
    <member>
      <identifier>8001</identifier>
    </member>
    <profile>
      <first_name>Xiomara</first_name>
      <last_name>Lane</last_name>
      <email>xiomara@example.com</email>
    </profile>
    <contact_details>
      <mobile>0400111222</mobile>
      <postcode>3001</postcode>
    </contact_details>
    <status>active</status>
    <source>manual-xml</source>
  </contact>
  <contact>
    <member>
      <identifier>8002</identifier>
    </member>
    <profile>
      <first_name>Yasmin</first_name>
      <last_name>Stone</last_name>
      <email>yasmin@example.com</email>
    </profile>
    <contact_details>
      <mobile>0400333444</mobile>
      <postcode>3002</postcode>
    </contact_details>
    <status>trial</status>
    <source>manual-xml</source>
  </contact>
</contacts>`
	if err := os.WriteFile(inputPath, []byte(xmlContent), 0o644); err != nil {
		t.Fatalf("failed to write xml fixture: %v", err)
	}

	mappingContent := `
files:
  - match:
      name: customers.xml
      format: xml
    xmlRecordPath: contacts.contact
    columns:
      member.identifier: Id
      profile.first_name: FirstName
      profile.last_name: LastName
      profile.email: Email
      contact_details.mobile: Mobile
      contact_details.postcode: PostCode
      source: DataSource
    attributes:
      status: SubscriptionStatus
`
	if err := os.WriteFile(mappingPath, []byte(strings.TrimSpace(mappingContent)), 0o644); err != nil {
		t.Fatalf("failed to write xml mapping file: %v", err)
	}

	summary, err := Run(context.Background(), &Config{
		Paths:           []string{inputPath},
		Workers:         1,
		LogPath:         logDir,
		OutputPath:      outputPath,
		MappingFile:     mappingPath,
		RequireMappings: true,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got, want := summary.RowsWritten, int64(2); got != want {
		t.Fatalf("expected %d rows written, got %d", want, got)
	}

	records := readUnifiedRecords(t, outputPath)
	if len(records) != 2 {
		t.Fatalf("expected 2 unified records, got %d", len(records))
	}
	if records[0].ID == nil || *records[0].ID != "8001" {
		t.Fatalf("expected first XML record Id 8001, got %+v", records[0].ID)
	}
	if records[1].Email == nil || *records[1].Email != "yasmin@example.com" {
		t.Fatalf("expected second XML record email to be preserved, got %+v", records[1].Email)
	}
	for _, record := range records {
		if record.DataSource == nil || *record.DataSource != "manual-xml" {
			t.Fatalf("expected XML source field to map into DataSource, got %+v", record.DataSource)
		}
		if !hasAttributeKey(record.Attributes, "SubscriptionStatus") {
			t.Fatalf("expected XML status to be mapped into attributes, got %+v", record.Attributes)
		}
	}
}

func TestRunSupportsCSVOutputForScalarRows(t *testing.T) {
	root := t.TempDir()
	logDir := filepath.Join(root, "logs")
	outputPath := filepath.Join(root, "out", "unified.csv")
	inputPath := filepath.Join(root, "contacts.json")
	mappingPath := filepath.Join(root, "mappings.yaml")

	jsonContent := `[
  {
    "identifier": "7001",
    "first_name": "Jamie",
    "last_name": "Fox",
    "email": "jamie@example.com",
    "mobile": "0400123000",
    "postcode": "3010",
    "source": "api"
  },
  {
    "identifier": "7002",
    "first_name": "Kai",
    "last_name": "Moore",
    "email": "kai@example.com",
    "mobile": "0400123001",
    "postcode": "3011",
    "source": "api"
  }
]`
	if err := os.WriteFile(inputPath, []byte(jsonContent), 0o644); err != nil {
		t.Fatalf("failed to write scalar json fixture: %v", err)
	}

	mappingContent := `
files:
  - match:
      name: contacts.json
      format: json
    columns:
      identifier: Id
      first_name: FirstName
      last_name: LastName
      email: Email
      mobile: Mobile
      postcode: PostCode
      source: DataSource
`
	if err := os.WriteFile(mappingPath, []byte(strings.TrimSpace(mappingContent)), 0o644); err != nil {
		t.Fatalf("failed to write csv output mapping file: %v", err)
	}

	summary, err := Run(context.Background(), &Config{
		Paths:           []string{inputPath},
		Workers:         1,
		LogPath:         logDir,
		OutputPath:      outputPath,
		MappingFile:     mappingPath,
		RequireMappings: true,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got, want := summary.RowsWritten, int64(2); got != want {
		t.Fatalf("expected %d rows written, got %d", want, got)
	}

	rows := readCSVRows(t, outputPath)
	if len(rows) != 3 {
		t.Fatalf("expected header plus 2 rows, got %d rows", len(rows))
	}
	if got, want := strings.Join(rows[0], ","), strings.Join(csvOutputHeaders, ","); got != want {
		t.Fatalf("expected CSV header %q, got %q", want, got)
	}
	if got, want := rows[1][0], "7001"; got != want {
		t.Fatalf("expected first CSV row Id %q, got %q", want, got)
	}
	if got, want := rows[1][6], "api"; got != want {
		t.Fatalf("expected first CSV row DataSource %q, got %q", want, got)
	}
}

func TestRunRejectsCSVOutputForComplexNestedRows(t *testing.T) {
	root := t.TempDir()
	logDir := filepath.Join(root, "logs")
	outputPath := filepath.Join(root, "out", "unified.csv")
	inputPath := filepath.Join(root, "contacts.csv")
	mappingPath := filepath.Join(root, "mappings.yaml")

	csvContent := "customer_id,first_name,email,status\n8101,Luca,luca@example.com,active\n"
	if err := os.WriteFile(inputPath, []byte(csvContent), 0o644); err != nil {
		t.Fatalf("failed to write csv fixture: %v", err)
	}

	mappingContent := `
files:
  - match:
      name: contacts.csv
      format: csv
    columns:
      customer_id: Id
      first_name: FirstName
      email: Email
    attributes:
      status: SubscriptionStatus
`
	if err := os.WriteFile(mappingPath, []byte(strings.TrimSpace(mappingContent)), 0o644); err != nil {
		t.Fatalf("failed to write complex csv output mapping file: %v", err)
	}

	summary, err := Run(context.Background(), &Config{
		Paths:           []string{inputPath},
		Workers:         1,
		LogPath:         logDir,
		OutputPath:      outputPath,
		MappingFile:     mappingPath,
		RequireMappings: true,
	})
	if err == nil {
		t.Fatal("expected CSV output run to fail for nested Attributes")
	}
	if !strings.Contains(err.Error(), ".json, .ndjson, or .jsonl") {
		t.Fatalf("expected CSV error to advise JSON output types, got %v", err)
	}
	if summary == nil {
		t.Fatal("expected summary to be returned alongside the error")
	}
	if _, statErr := os.Stat(outputPath); !os.IsNotExist(statErr) {
		t.Fatalf("expected CSV output file to not be committed on failure, got stat error %v", statErr)
	}
}

func TestRunSupportsJSONOutputForNestedRows(t *testing.T) {
	root := t.TempDir()
	logDir := filepath.Join(root, "logs")
	outputPath := filepath.Join(root, "out", "unified.json")
	inputPath := filepath.Join(root, "profiles.json")
	mappingPath := filepath.Join(root, "mappings.yaml")

	jsonContent := `[
  {
    "identifier": "8201",
    "email": "nested@example.com",
    "source": "api",
    "status": "active"
  }
]`
	if err := os.WriteFile(inputPath, []byte(jsonContent), 0o644); err != nil {
		t.Fatalf("failed to write nested json fixture: %v", err)
	}

	mappingContent := `
files:
  - match:
      name: profiles.json
      format: json
    columns:
      identifier: Id
      email: Email
      source: DataSource
    attributes:
      status: SubscriptionStatus
`
	if err := os.WriteFile(mappingPath, []byte(strings.TrimSpace(mappingContent)), 0o644); err != nil {
		t.Fatalf("failed to write json output mapping file: %v", err)
	}

	summary, err := Run(context.Background(), &Config{
		Paths:           []string{inputPath},
		Workers:         1,
		LogPath:         logDir,
		OutputPath:      outputPath,
		MappingFile:     mappingPath,
		RequireMappings: true,
	})
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if got, want := summary.RowsWritten, int64(1); got != want {
		t.Fatalf("expected %d row written, got %d", want, got)
	}

	content, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("failed to read json output: %v", err)
	}
	var records []unifiedRecord
	if err := json.Unmarshal(content, &records); err != nil {
		t.Fatalf("failed to decode JSON array output: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 JSON output record, got %d", len(records))
	}
	if !hasAttribute(records[0].Attributes, "SubscriptionStatus", "active") {
		t.Fatalf("expected nested JSON output to preserve attributes, got %+v", records[0].Attributes)
	}
}

func readUnifiedRecords(t *testing.T, path string) []unifiedRecord {
	t.Helper()

	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read unified output: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	records := make([]unifiedRecord, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		var record unifiedRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("failed to decode unified record: %v", err)
		}
		records = append(records, record)
	}

	return records
}

func readCSVRows(t *testing.T, path string) [][]string {
	t.Helper()

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open csv output: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("failed to read csv output: %v", err)
	}
	return rows
}

func hasAttribute(attributes []unifiedAttribute, key, value string) bool {
	for _, attribute := range attributes {
		if attribute.Key == key && attribute.Value == value {
			return true
		}
	}
	return false
}

func hasAttributeKey(attributes []unifiedAttribute, key string) bool {
	for _, attribute := range attributes {
		if attribute.Key == key {
			return true
		}
	}
	return false
}
