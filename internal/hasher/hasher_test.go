package hasher

import (
	"sync"
	"testing"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/report"
)

func TestNewSelectiveHasher(t *testing.T) {
	strategy := config.HashingStrategy{
		Mode: "full_row",
	}

	hasher := NewSelectiveHasher(strategy)

	if hasher == nil {
		t.Fatal("Expected hasher to be created")
	}
	if hasher.strategy.Mode != "full_row" {
		t.Errorf("Expected strategy mode 'full_row', got '%s'", hasher.strategy.Mode)
	}
}

func TestSelectiveHasher_HashFullRow(t *testing.T) {
	strategy := config.HashingStrategy{
		Mode: "full_row",
	}
	hasher := NewSelectiveHasher(strategy)

	data := report.JSONData{
		"id":   "123",
		"name": "test",
		"age":  30,
	}

	hash1 := hasher.HashRow(data)
	hash2 := hasher.HashRow(data)

	if hash1 == "" {
		t.Error("Expected non-empty hash")
	}
	if hash1 != hash2 {
		t.Error("Expected consistent hashes for same data")
	}

	// Test with different data
	data2 := report.JSONData{
		"id":   "456",
		"name": "test2",
		"age":  25,
	}

	hash3 := hasher.HashRow(data2)
	if hash1 == hash3 {
		t.Error("Expected different hashes for different data")
	}
}

func TestSelectiveHasher_HashSelectedKeys(t *testing.T) {
	strategy := config.HashingStrategy{
		Mode:        "selective",
		IncludeKeys: []string{"id", "name"},
	}
	hasher := NewSelectiveHasher(strategy)

	data1 := report.JSONData{
		"id":        "123",
		"name":      "test",
		"age":       30,
		"timestamp": "2023-01-01",
	}

	data2 := report.JSONData{
		"id":        "123",
		"name":      "test",
		"age":       25,           // Different age
		"timestamp": "2023-01-02", // Different timestamp
	}

	hash1 := hasher.HashRow(data1)
	hash2 := hasher.HashRow(data2)

	if hash1 != hash2 {
		t.Error("Expected same hashes when only selected keys are considered")
	}
}

func TestSelectiveHasher_HashExcludingKeys(t *testing.T) {
	strategy := config.HashingStrategy{
		Mode:        "exclude_keys",
		ExcludeKeys: []string{"timestamp", "version"},
	}
	hasher := NewSelectiveHasher(strategy)

	data1 := report.JSONData{
		"id":        "123",
		"name":      "test",
		"age":       30,
		"timestamp": "2023-01-01",
		"version":   1,
	}

	data2 := report.JSONData{
		"id":        "123",
		"name":      "test",
		"age":       30,
		"timestamp": "2023-01-02", // Different timestamp
		"version":   2,            // Different version
	}

	hash1 := hasher.HashRow(data1)
	hash2 := hasher.HashRow(data2)

	if hash1 != hash2 {
		t.Error("Expected same hashes when excluded keys are different")
	}
}

func TestSelectiveHasher_HashSelectedKeysWithMissingKeys(t *testing.T) {
	strategy := config.HashingStrategy{
		Mode:        "selective",
		IncludeKeys: []string{"id", "name", "missing"},
	}
	hasher := NewSelectiveHasher(strategy)

	data := report.JSONData{
		"id":   "123",
		"name": "test",
		"age":  30,
	}

	hash := hasher.HashRow(data)
	if hash == "" {
		t.Error("Expected non-empty hash even with missing keys")
	}
}

func TestSelectiveHasher_NormalizeForHashing(t *testing.T) {
	strategy := config.HashingStrategy{Mode: "full_row"}
	hasher := NewSelectiveHasher(strategy)

	// Test map normalization (key ordering)
	data := map[string]any{
		"z": "value1",
		"a": "value2",
		"m": "value3",
	}

	normalized := hasher.normalizeForHashing(data)
	normalizedMap, ok := normalized.(map[string]any)
	if !ok {
		t.Fatal("Expected normalized data to be a map")
	}

	if len(normalizedMap) != 3 {
		t.Errorf("Expected 3 keys in normalized map, got %d", len(normalizedMap))
	}

	// Test array normalization
	arrayData := []any{
		map[string]any{"b": 2, "a": 1},
		"string",
		42,
	}

	normalizedArray := hasher.normalizeForHashing(arrayData)
	if normalizedArray == nil {
		t.Error("Expected normalized array to not be nil")
	}
}

func TestSelectiveHasher_HashRowConcurrentStability(t *testing.T) {
	strategy := config.HashingStrategy{
		Mode:        "selective",
		IncludeKeys: []string{"id", "name", "metadata"},
	}
	hasher := NewSelectiveHasher(strategy)

	data := report.JSONData{
		"id":   "123",
		"name": "test",
		"metadata": map[string]any{
			"region": "apac",
			"tags":   []any{"one", "two", "three"},
		},
		"ignored": "value",
	}

	expected := hasher.HashRow(data)
	if expected == "" {
		t.Fatal("Expected non-empty baseline hash")
	}

	const goroutines = 32
	const iterations = 200

	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan string, goroutines*iterations)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				results <- hasher.HashRow(data)
			}
		}()
	}

	close(start)
	wg.Wait()
	close(results)

	for result := range results {
		if result != expected {
			t.Fatalf("expected stable concurrent hash %q, got %q", expected, result)
		}
	}
}

func TestNewAdvancedHasher(t *testing.T) {
	primary := config.HashingStrategy{Mode: "full_row"}
	secondary := config.HashingStrategy{
		Mode:        "exclude_keys",
		ExcludeKeys: []string{"timestamp"},
	}

	hasher := NewAdvancedHasher(primary, secondary)

	if hasher == nil {
		t.Fatal("Expected advanced hasher to be created")
	}
	if hasher.primary == nil {
		t.Error("Expected primary hasher to be initialized")
	}
	if hasher.secondary == nil {
		t.Error("Expected secondary hasher to be initialized")
	}
}

func TestAdvancedHasher_HashRowDual(t *testing.T) {
	primary := config.HashingStrategy{Mode: "full_row"}
	secondary := config.HashingStrategy{
		Mode:        "exclude_keys",
		ExcludeKeys: []string{"timestamp"},
	}
	hasher := NewAdvancedHasher(primary, secondary)

	data := report.JSONData{
		"id":        "123",
		"name":      "test",
		"timestamp": "2023-01-01",
	}

	hash1, hash2 := hasher.HashRowDual(data)

	if hash1 == "" || hash2 == "" {
		t.Error("Expected both hashes to be non-empty")
	}
	if hash1 == hash2 {
		t.Error("Expected primary and secondary hashes to be different")
	}
}

func TestAdvancedHasher_HashRowComposite(t *testing.T) {
	primary := config.HashingStrategy{Mode: "full_row"}
	secondary := config.HashingStrategy{
		Mode:        "exclude_keys",
		ExcludeKeys: []string{"timestamp"},
	}
	hasher := NewAdvancedHasher(primary, secondary)

	data := report.JSONData{
		"id":   "123",
		"name": "test",
	}

	composite1 := hasher.HashRowComposite(data)
	composite2 := hasher.HashRowComposite(data)

	if composite1 == "" {
		t.Error("Expected non-empty composite hash")
	}
	if composite1 != composite2 {
		t.Error("Expected consistent composite hashes")
	}
}

func TestNewSemanticHasher(t *testing.T) {
	hasher := NewSemanticHasher(true, true, true)

	if hasher == nil {
		t.Fatal("Expected semantic hasher to be created")
	}
	if !hasher.normalizeStrings {
		t.Error("Expected normalizeStrings to be true")
	}
	if !hasher.ignoreWhitespace {
		t.Error("Expected ignoreWhitespace to be true")
	}
	if !hasher.ignoreCase {
		t.Error("Expected ignoreCase to be true")
	}
}

func TestSemanticHasher_NormalizeString(t *testing.T) {
	hasher := NewSemanticHasher(true, true, true)

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"basic", "Hello World", "hello world"},
		{"extra whitespace", "Hello    World   ", "hello world"},
		{"mixed case", "HeLLo WoRLd", "hello world"},
		{"leading/trailing spaces", "  Hello World  ", "hello world"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasher.normalizeString(tt.input)
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestSemanticHasher_NormalizeStringFlags(t *testing.T) {
	// Test with different flag combinations
	hasher1 := NewSemanticHasher(false, false, false)
	result1 := hasher1.normalizeString("  Hello World  ")
	if result1 != "  Hello World  " {
		t.Errorf("Expected no normalization, got '%s'", result1)
	}

	hasher2 := NewSemanticHasher(false, false, true)
	result2 := hasher2.normalizeString("Hello World")
	if result2 != "hello world" {
		t.Errorf("Expected case normalization only, got '%s'", result2)
	}

	hasher3 := NewSemanticHasher(false, true, false)
	result3 := hasher3.normalizeString("Hello    World")
	if result3 != "Hello World" {
		t.Errorf("Expected whitespace normalization only, got '%s'", result3)
	}
}

func TestSemanticHasher_HashRowSemantic(t *testing.T) {
	hasher := NewSemanticHasher(true, true, true)
	strategy := config.HashingStrategy{Mode: "full_row"}

	data1 := report.JSONData{
		"name": "Hello World",
		"desc": "Test Description",
	}

	data2 := report.JSONData{
		"name": "HELLO    WORLD",
		"desc": "  test description  ",
	}

	hash1 := hasher.HashRowSemantic(data1, strategy)
	hash2 := hasher.HashRowSemantic(data2, strategy)

	if hash1 != hash2 {
		t.Error("Expected same semantic hashes for similar content")
	}
}

func TestSemanticHasher_NormalizeValue(t *testing.T) {
	hasher := NewSemanticHasher(true, true, true)

	// Test string normalization
	result := hasher.NormalizeValue("  Hello World  ")
	if result != "hello world" {
		t.Errorf("Expected normalized string, got '%s'", result)
	}

	// Test map normalization
	mapValue := map[string]any{
		"name": "  TEST  ",
		"age":  30,
	}

	normalizedMap := hasher.NormalizeValue(mapValue)
	normalizedMapTyped, ok := normalizedMap.(map[string]any)
	if !ok {
		t.Fatal("Expected normalized map")
	}

	if normalizedMapTyped["name"] != "test" {
		t.Errorf("Expected normalized name, got '%s'", normalizedMapTyped["name"])
	}

	// Test array normalization
	arrayValue := []any{"  TEST  ", 42}
	normalizedArray := hasher.NormalizeValue(arrayValue)
	normalizedArrayTyped, ok := normalizedArray.([]any)
	if !ok {
		t.Fatal("Expected normalized array")
	}

	if normalizedArrayTyped[0] != "test" {
		t.Errorf("Expected normalized array element, got '%s'", normalizedArrayTyped[0])
	}
}

func TestNewMetricsCollector(t *testing.T) {
	collector := NewMetricsCollector()

	if collector == nil {
		t.Fatal("Expected metrics collector to be created")
	}
	if collector.metrics.HashDistribution == nil {
		t.Error("Expected hash distribution map to be initialized")
	}
	if collector.metrics.StrategyBreakdown == nil {
		t.Error("Expected strategy breakdown map to be initialized")
	}
	if collector.hashes == nil {
		t.Error("Expected hashes map to be initialized")
	}
}

func TestMetricsCollector_RecordHash(t *testing.T) {
	collector := NewMetricsCollector()

	// Record first hash
	collector.RecordHash("hash1", "full_row")

	if collector.metrics.TotalRows != 1 {
		t.Errorf("Expected total rows 1, got %d", collector.metrics.TotalRows)
	}
	if collector.metrics.UniqueHashes != 1 {
		t.Errorf("Expected unique hashes 1, got %d", collector.metrics.UniqueHashes)
	}
	if collector.metrics.StrategyBreakdown["full_row"] != 1 {
		t.Errorf("Expected strategy breakdown 1, got %d", collector.metrics.StrategyBreakdown["full_row"])
	}

	// Record same hash again
	collector.RecordHash("hash1", "full_row")

	if collector.metrics.TotalRows != 2 {
		t.Errorf("Expected total rows 2, got %d", collector.metrics.TotalRows)
	}
	if collector.metrics.UniqueHashes != 1 {
		t.Errorf("Expected unique hashes still 1, got %d", collector.metrics.UniqueHashes)
	}

	// Record different hash
	collector.RecordHash("hash2", "selective")

	if collector.metrics.TotalRows != 3 {
		t.Errorf("Expected total rows 3, got %d", collector.metrics.TotalRows)
	}
	if collector.metrics.UniqueHashes != 2 {
		t.Errorf("Expected unique hashes 2, got %d", collector.metrics.UniqueHashes)
	}
	if collector.metrics.StrategyBreakdown["selective"] != 1 {
		t.Errorf("Expected selective strategy count 1, got %d", collector.metrics.StrategyBreakdown["selective"])
	}
}

func TestMetricsCollector_GetMetrics(t *testing.T) {
	collector := NewMetricsCollector()

	// Add some test data
	collector.RecordHash("hash1", "full_row")
	collector.RecordHash("hash1", "full_row")
	collector.RecordHash("hash2", "selective")
	collector.RecordHash("hash3", "selective")

	metrics := collector.GetMetrics()

	if metrics.TotalRows != 4 {
		t.Errorf("Expected total rows 4, got %d", metrics.TotalRows)
	}
	if metrics.UniqueHashes != 3 {
		t.Errorf("Expected unique hashes 3, got %d", metrics.UniqueHashes)
	}

	expectedCollisionRate := 1.0 - (3.0 / 4.0) // 0.25
	if metrics.CollisionRate != expectedCollisionRate {
		t.Errorf("Expected collision rate %f, got %f", expectedCollisionRate, metrics.CollisionRate)
	}

	// Check hash distribution (only collisions are included)
	if metrics.HashDistribution["hash1"] != 2 {
		t.Errorf("Expected hash1 distribution 2, got %d", metrics.HashDistribution["hash1"])
	}
	if _, exists := metrics.HashDistribution["hash2"]; exists {
		t.Error("Expected hash2 not in distribution (no collision)")
	}
}

func TestValidateHashingStrategy(t *testing.T) {
	tests := []struct {
		name      string
		strategy  config.HashingStrategy
		wantError bool
	}{
		{
			name:      "valid full_row",
			strategy:  config.HashingStrategy{Mode: "full_row"},
			wantError: false,
		},
		{
			name: "valid selective",
			strategy: config.HashingStrategy{
				Mode:        "selective",
				IncludeKeys: []string{"id", "name"},
			},
			wantError: false,
		},
		{
			name: "invalid selective (no keys)",
			strategy: config.HashingStrategy{
				Mode:        "selective",
				IncludeKeys: []string{},
			},
			wantError: true,
		},
		{
			name: "valid exclude_keys",
			strategy: config.HashingStrategy{
				Mode:        "exclude_keys",
				ExcludeKeys: []string{"timestamp"},
			},
			wantError: false,
		},
		{
			name: "invalid exclude_keys (no keys)",
			strategy: config.HashingStrategy{
				Mode:        "exclude_keys",
				ExcludeKeys: []string{},
			},
			wantError: true,
		},
		{
			name:      "invalid mode",
			strategy:  config.HashingStrategy{Mode: "invalid"},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateHashingStrategy(tt.strategy)
			if tt.wantError && err == nil {
				t.Error("Expected error for invalid strategy")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
		})
	}
}

func TestOptimizeHashingStrategy(t *testing.T) {
	strategy := config.HashingStrategy{
		Mode:        "exclude_keys",
		ExcludeKeys: []string{"version"},
	}

	// Sample data with high variance in timestamp
	sampleData := []report.JSONData{
		{"id": "1", "name": "test", "timestamp": "2023-01-01T00:00:00Z"},
		{"id": "2", "name": "test", "timestamp": "2023-01-01T00:01:00Z"},
		{"id": "3", "name": "test", "timestamp": "2023-01-01T00:02:00Z"},
		{"id": "4", "name": "test", "timestamp": "2023-01-01T00:03:00Z"},
		{"id": "5", "name": "test", "timestamp": "2023-01-01T00:04:00Z"},
	}

	optimized := OptimizeHashingStrategy(strategy, sampleData)

	if optimized.Mode != "exclude_keys" {
		t.Errorf("Expected mode to remain 'exclude_keys', got '%s'", optimized.Mode)
	}

	// The optimization should potentially add high-variance keys
	if len(optimized.ExcludeKeys) < len(strategy.ExcludeKeys) {
		t.Error("Expected optimization to potentially add more exclude keys")
	}
}

func TestAnalyzeKeyVariance(t *testing.T) {
	sampleData := []report.JSONData{
		{"id": "1", "name": "test", "timestamp": "2023-01-01T00:00:00Z"},
		{"id": "2", "name": "test", "timestamp": "2023-01-01T00:01:00Z"},
		{"id": "3", "name": "test", "timestamp": "2023-01-01T00:02:00Z"},
	}

	variance := analyzeKeyVariance(sampleData)

	if variance == nil {
		t.Fatal("Expected variance map to be created")
	}

	// ID should have high variance (all unique)
	if variance["id"] != 1.0 {
		t.Errorf("Expected ID variance 1.0, got %f", variance["id"])
	}

	// Name should have low variance (all same)
	if variance["name"] != 1.0/3.0 {
		t.Errorf("Expected name variance %f, got %f", 1.0/3.0, variance["name"])
	}

	// Timestamp should have high variance (all unique)
	if variance["timestamp"] != 1.0 {
		t.Errorf("Expected timestamp variance 1.0, got %f", variance["timestamp"])
	}
}

func TestHashConsistency(t *testing.T) {
	// Test that different key orders produce same hash
	strategy := config.HashingStrategy{Mode: "full_row"}
	hasher := NewSelectiveHasher(strategy)

	data1 := report.JSONData{
		"id":   "123",
		"name": "test",
		"age":  30,
	}

	data2 := report.JSONData{
		"age":  30,
		"name": "test",
		"id":   "123",
	}

	hash1 := hasher.HashRow(data1)
	hash2 := hasher.HashRow(data2)

	if hash1 != hash2 {
		t.Error("Expected consistent hashes regardless of key order")
	}
}
