// internal/processing/row_processor_test.go
package processing

import (
	"fmt"
	"hash/fnv"
	"testing"

	"github.com/benjaminwestern/dupe-analyser/internal/report"
)

func TestNewRowProcessor(t *testing.T) {
	config := &RowProcessorConfig{
		UniqueKey:    "id",
		CheckKey:     true,
		CheckRow:     true,
		ValidateOnly: false,
	}

	processor := NewRowProcessor(config)

	if processor == nil {
		t.Fatal("Expected non-nil processor")
	}

	if processor.config != config {
		t.Error("Expected config to be set")
	}

	if processor.idLocations == nil {
		t.Error("Expected idLocations to be initialized")
	}

	if processor.rowHashes == nil {
		t.Error("Expected rowHashes to be initialized")
	}
}

func TestRowProcessor_ProcessRow_KeyCheck(t *testing.T) {
	config := &RowProcessorConfig{
		UniqueKey:    "id",
		CheckKey:     true,
		CheckRow:     false,
		ValidateOnly: false,
	}

	processor := NewRowProcessor(config)

	data := report.JSONData{
		"id":   "test-id",
		"name": "test-name",
	}

	location := report.LocationInfo{
		FilePath:   "/test/file.json",
		LineNumber: 1,
	}

	hasher := fnv.New64a()
	err := processor.ProcessRow(data, location, hasher)

	if err != nil {
		t.Fatalf("ProcessRow failed: %v", err)
	}

	// Check that key was found
	keysFound := processor.GetKeysFoundPerFolder()
	if keysFound["/test"] != 1 {
		t.Errorf("Expected 1 key found, got %d", keysFound["/test"])
	}

	// Check that ID was stored
	idLocations := processor.GetIDLocations()
	if len(idLocations["test-id"]) != 1 {
		t.Errorf("Expected 1 location for test-id, got %d", len(idLocations["test-id"]))
	}
}

func TestRowProcessor_ProcessRow_RowHash(t *testing.T) {
	config := &RowProcessorConfig{
		UniqueKey:    "id",
		CheckKey:     false,
		CheckRow:     true,
		ValidateOnly: false,
	}

	processor := NewRowProcessor(config)

	data := report.JSONData{
		"id":   "test-id",
		"name": "test-name",
	}

	location := report.LocationInfo{
		FilePath:   "/test/file.json",
		LineNumber: 1,
	}

	hasher := fnv.New64a()
	err := processor.ProcessRow(data, location, hasher)

	if err != nil {
		t.Fatalf("ProcessRow failed: %v", err)
	}

	// Check that row hash was stored
	rowHashes := processor.GetRowHashes()
	if len(rowHashes) != 1 {
		t.Errorf("Expected 1 row hash, got %d", len(rowHashes))
	}

	// Process the same data again to test duplicate detection
	location2 := report.LocationInfo{
		FilePath:   "/test/file2.json",
		LineNumber: 1,
	}

	err = processor.ProcessRow(data, location2, hasher)
	if err != nil {
		t.Fatalf("ProcessRow failed: %v", err)
	}

	// Check that the same hash now has 2 locations
	rowHashes = processor.GetRowHashes()
	if len(rowHashes) != 1 {
		t.Errorf("Expected 1 unique row hash, got %d", len(rowHashes))
	}

	for _, locations := range rowHashes {
		if len(locations) != 2 {
			t.Errorf("Expected 2 locations for duplicate row, got %d", len(locations))
		}
	}
}

func TestRowProcessor_ProcessRow_ValidateOnly(t *testing.T) {
	config := &RowProcessorConfig{
		UniqueKey:    "id",
		CheckKey:     true,
		CheckRow:     false,
		ValidateOnly: true,
	}

	processor := NewRowProcessor(config)

	data := report.JSONData{
		"id":   "test-id",
		"name": "test-name",
	}

	location := report.LocationInfo{
		FilePath:   "/test/file.json",
		LineNumber: 1,
	}

	hasher := fnv.New64a()
	err := processor.ProcessRow(data, location, hasher)

	if err != nil {
		t.Fatalf("ProcessRow failed: %v", err)
	}

	// Check that key was found
	keysFound := processor.GetKeysFoundPerFolder()
	if keysFound["/test"] != 1 {
		t.Errorf("Expected 1 key found, got %d", keysFound["/test"])
	}

	// Check that ID was NOT stored (validate only mode)
	idLocations := processor.GetIDLocations()
	if len(idLocations) != 0 {
		t.Errorf("Expected no ID locations in validate only mode, got %d", len(idLocations))
	}
}

func TestRowProcessor_ProcessRow_NoKey(t *testing.T) {
	config := &RowProcessorConfig{
		UniqueKey:    "id",
		CheckKey:     true,
		CheckRow:     false,
		ValidateOnly: false,
	}

	processor := NewRowProcessor(config)

	data := report.JSONData{
		"name": "test-name",
		// No "id" key
	}

	location := report.LocationInfo{
		FilePath:   "/test/file.json",
		LineNumber: 1,
	}

	hasher := fnv.New64a()
	err := processor.ProcessRow(data, location, hasher)

	if err != nil {
		t.Fatalf("ProcessRow failed: %v", err)
	}

	// Check that no key was found
	keysFound := processor.GetKeysFoundPerFolder()
	if keysFound["/test"] != 0 {
		t.Errorf("Expected 0 keys found, got %d", keysFound["/test"])
	}

	// Check that no ID was stored
	idLocations := processor.GetIDLocations()
	if len(idLocations) != 0 {
		t.Errorf("Expected no ID locations, got %d", len(idLocations))
	}
}

func TestRowProcessor_Close(t *testing.T) {
	config := &RowProcessorConfig{
		UniqueKey:    "id",
		CheckKey:     true,
		CheckRow:     true,
		ValidateOnly: false,
	}

	processor := NewRowProcessor(config)

	// Add some test data
	data := report.JSONData{
		"id":   "test-id",
		"name": "test-name",
	}

	location := report.LocationInfo{
		FilePath:   "/test/file.json",
		LineNumber: 1,
	}

	hasher := fnv.New64a()
	processor.ProcessRow(data, location, hasher)

	// Verify data exists
	if len(processor.GetIDLocations()) == 0 {
		t.Error("Expected ID locations before close")
	}

	// Close the processor
	err := processor.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify data is cleared
	if len(processor.GetIDLocations()) != 0 {
		t.Error("Expected ID locations to be cleared after close")
	}

	if len(processor.GetRowHashes()) != 0 {
		t.Error("Expected row hashes to be cleared after close")
	}

	if len(processor.GetKeysFoundPerFolder()) != 0 {
		t.Error("Expected keys found per folder to be cleared after close")
	}

	if len(processor.GetRowsProcessedPerFolder()) != 0 {
		t.Error("Expected rows processed per folder to be cleared after close")
	}
}

func TestRowProcessor_hashMapToHasher(t *testing.T) {
	config := &RowProcessorConfig{
		UniqueKey: "id",
		CheckKey:  false,
		CheckRow:  true,
	}

	processor := NewRowProcessor(config)

	data1 := map[string]interface{}{
		"id":   "test-id",
		"name": "test-name",
	}

	data2 := map[string]interface{}{
		"name": "test-name",
		"id":   "test-id",
	}

	hasher1 := fnv.New64a()
	hasher2 := fnv.New64a()

	err1 := processor.hashMapToHasher(data1, hasher1)
	err2 := processor.hashMapToHasher(data2, hasher2)

	if err1 != nil || err2 != nil {
		t.Fatalf("hashMapToHasher failed: %v, %v", err1, err2)
	}

	// Hash should be the same regardless of key order
	if hasher1.Sum64() != hasher2.Sum64() {
		t.Error("Expected consistent hashing regardless of key order")
	}
}

func TestRowProcessor_GetStatistics(t *testing.T) {
	config := &RowProcessorConfig{
		UniqueKey:    "id",
		CheckKey:     true,
		CheckRow:     true,
		ValidateOnly: false,
	}

	processor := NewRowProcessor(config)

	// Process multiple rows
	for i := 0; i < 5; i++ {
		data := report.JSONData{
			"id":   fmt.Sprintf("test-id-%d", i),
			"name": "test-name",
		}

		location := report.LocationInfo{
			FilePath:   "/test/file.json",
			LineNumber: i + 1,
		}

		hasher := fnv.New64a()
		processor.ProcessRow(data, location, hasher)
	}

	// Check statistics
	keysFound := processor.GetKeysFoundPerFolder()
	if keysFound["/test"] != 5 {
		t.Errorf("Expected 5 keys found, got %d", keysFound["/test"])
	}

	rowsProcessed := processor.GetRowsProcessedPerFolder()
	if rowsProcessed["/test"] != 5 {
		t.Errorf("Expected 5 rows processed, got %d", rowsProcessed["/test"])
	}

	idLocations := processor.GetIDLocations()
	if len(idLocations) != 5 {
		t.Errorf("Expected 5 unique IDs, got %d", len(idLocations))
	}

	rowHashes := processor.GetRowHashes()
	if len(rowHashes) != 5 {
		t.Errorf("Expected 5 unique row hashes, got %d", len(rowHashes))
	}
}
