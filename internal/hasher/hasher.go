// Package hasher provides row-hashing strategies and hashing metrics helpers.
package hasher

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/report"
)

const (
	modeFullRow     = "full_row"
	modeSelective   = "selective"
	modeExcludeKeys = "exclude_keys"
)

// SelectiveHasher provides configurable hashing strategies.
type SelectiveHasher struct {
	strategy config.HashingStrategy
}

// NewSelectiveHasher creates a new selective hasher with the given strategy.
func NewSelectiveHasher(strategy config.HashingStrategy) *SelectiveHasher {
	return &SelectiveHasher{
		strategy: strategy,
	}
}

// HashRow hashes a row based on the configured strategy.
func (sh *SelectiveHasher) HashRow(data report.JSONData) string {
	switch sh.strategy.Mode {
	case modeFullRow:
		return sh.hashFullRow(data)
	case modeSelective:
		return sh.hashSelectedKeys(data, sh.strategy.IncludeKeys)
	case modeExcludeKeys:
		return sh.hashExcludingKeys(data, sh.strategy.ExcludeKeys)
	default:
		return sh.hashFullRow(data)
	}
}

// hashFullRow hashes the entire JSON object (default behavior).
func (sh *SelectiveHasher) hashFullRow(data report.JSONData) string {
	// Convert to JSON for consistent hashing
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		// Fallback to string representation
		jsonBytes = []byte(fmt.Sprintf("%v", data))
	}

	return hashBytesFNV(jsonBytes)
}

// hashSelectedKeys hashes only the specified keys.
func (sh *SelectiveHasher) hashSelectedKeys(data report.JSONData, keys []string) string {
	// Create a new map with only the selected keys
	selectedData := make(map[string]any)
	for _, key := range keys {
		if value, exists := data[key]; exists {
			selectedData[key] = value
		}
	}

	// Convert to consistent JSON representation
	jsonBytes, err := sh.marshalConsistently(selectedData)
	if err != nil {
		jsonBytes = []byte(fmt.Sprintf("%v", selectedData))
	}

	return hashBytesFNV(jsonBytes)
}

// hashExcludingKeys hashes everything except the specified keys.
func (sh *SelectiveHasher) hashExcludingKeys(data report.JSONData, excludeKeys []string) string {
	// Create exclusion set for fast lookup
	excludeSet := make(map[string]bool)
	for _, key := range excludeKeys {
		excludeSet[key] = true
	}

	// Create a new map excluding the specified keys
	filteredData := make(map[string]any)
	for key, value := range data {
		if !excludeSet[key] {
			filteredData[key] = value
		}
	}

	// Convert to consistent JSON representation
	jsonBytes, err := sh.marshalConsistently(filteredData)
	if err != nil {
		jsonBytes = []byte(fmt.Sprintf("%v", filteredData))
	}

	return hashBytesFNV(jsonBytes)
}

func hashBytesFNV(data []byte) string {
	hasher := fnv.New64a()
	if _, err := hasher.Write(data); err != nil {
		sum := sha256.Sum256(data)
		return fmt.Sprintf("%x", sum[:])
	}
	return strconv.FormatUint(hasher.Sum64(), 10)
}

// marshalConsistently marshals data in a consistent way for hashing.
func (sh *SelectiveHasher) marshalConsistently(data any) ([]byte, error) {
	// For consistent hashing, we need to ensure key ordering
	normalized := sh.normalizeForHashing(data)
	normalizedBytes, err := json.Marshal(normalized)
	if err != nil {
		return nil, fmt.Errorf("marshal normalized hash data: %w", err)
	}

	return normalizedBytes, nil
}

// normalizeForHashing normalizes data structure for consistent hashing.
func (sh *SelectiveHasher) normalizeForHashing(data any) any {
	switch v := data.(type) {
	case map[string]any:
		// Sort keys for consistent ordering
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		normalized := make(map[string]any)
		for _, k := range keys {
			normalized[k] = sh.normalizeForHashing(v[k])
		}
		return normalized

	case []any:
		// Normalize array elements
		normalized := make([]any, len(v))
		for i, item := range v {
			normalized[i] = sh.normalizeForHashing(item)
		}
		return normalized

	default:
		return v
	}
}

// AdvancedHasher provides additional hashing capabilities.
type AdvancedHasher struct {
	primary   *SelectiveHasher
	secondary *SelectiveHasher
}

// NewAdvancedHasher creates a hasher with primary and secondary strategies.
func NewAdvancedHasher(primary, secondary config.HashingStrategy) *AdvancedHasher {
	return &AdvancedHasher{
		primary:   NewSelectiveHasher(primary),
		secondary: NewSelectiveHasher(secondary),
	}
}

// HashRowDual generates both primary and secondary hashes.
func (ah *AdvancedHasher) HashRowDual(data report.JSONData) (string, string) {
	primaryHash := ah.primary.HashRow(data)
	secondaryHash := ah.secondary.HashRow(data)
	return primaryHash, secondaryHash
}

// HashRowComposite generates a composite hash from multiple strategies.
func (ah *AdvancedHasher) HashRowComposite(data report.JSONData) string {
	primary, secondary := ah.HashRowDual(data)

	// Combine hashes using a stronger digest to avoid weak-primitive warnings.
	sum := sha256.Sum256([]byte(primary + ":" + secondary))
	return fmt.Sprintf("%x", sum[:])
}

// SemanticHasher provides semantic hashing for similar content detection.
type SemanticHasher struct {
	normalizeStrings bool
	ignoreWhitespace bool
	ignoreCase       bool
}

// NewSemanticHasher creates a semantic hasher.
func NewSemanticHasher(normalizeStrings, ignoreWhitespace, ignoreCase bool) *SemanticHasher {
	return &SemanticHasher{
		normalizeStrings: normalizeStrings,
		ignoreWhitespace: ignoreWhitespace,
		ignoreCase:       ignoreCase,
	}
}

// HashRowSemantic generates a semantic hash for content similarity.
func (sh *SemanticHasher) HashRowSemantic(data report.JSONData, strategy config.HashingStrategy) string {
	hasher := NewSelectiveHasher(strategy)

	// Normalize the data for semantic comparison
	normalizedData := sh.normalizeData(data)

	return hasher.HashRow(normalizedData)
}

// normalizeData applies semantic normalization to data.
func (sh *SemanticHasher) normalizeData(data report.JSONData) report.JSONData {
	normalized := make(report.JSONData)

	for key, value := range data {
		normalized[key] = sh.NormalizeValue(value)
	}

	return normalized
}

// NormalizeValue normalizes individual values based on semantic rules.
func (sh *SemanticHasher) NormalizeValue(value any) any {
	switch v := value.(type) {
	case string:
		return sh.normalizeString(v)
	case map[string]any:
		normalized := make(map[string]any)
		for key, value := range v {
			normalized[key] = sh.NormalizeValue(value)
		}
		return normalized
	case []any:
		normalized := make([]any, len(v))
		for i, item := range v {
			normalized[i] = sh.NormalizeValue(item)
		}
		return normalized
	default:
		return value
	}
}

// normalizeString applies string normalization rules.
func (sh *SemanticHasher) normalizeString(s string) string {
	if sh.ignoreCase {
		s = strings.ToLower(s)
	}

	if sh.ignoreWhitespace {
		s = strings.Join(strings.Fields(s), " ")
	}

	if sh.normalizeStrings {
		// Additional normalization (trim, etc.)
		s = strings.TrimSpace(s)
	}

	return s
}

// HashingMetrics provides statistics about hashing performance.
type HashingMetrics struct {
	TotalRows         int64            `json:"totalRows"`
	UniqueHashes      int64            `json:"uniqueHashes"`
	CollisionRate     float64          `json:"collisionRate"`
	AverageHashTime   float64          `json:"averageHashTime"`
	HashDistribution  map[string]int64 `json:"hashDistribution"`
	StrategyBreakdown map[string]int64 `json:"strategyBreakdown"`
}

// MetricsCollector collects hashing performance metrics.
type MetricsCollector struct {
	metrics HashingMetrics
	hashes  map[string]int64
}

// NewMetricsCollector creates a new metrics collector.
func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		metrics: HashingMetrics{
			HashDistribution:  make(map[string]int64),
			StrategyBreakdown: make(map[string]int64),
		},
		hashes: make(map[string]int64),
	}
}

// RecordHash records a hash for metrics collection.
func (mc *MetricsCollector) RecordHash(hash string, strategy string) {
	mc.metrics.TotalRows++
	mc.hashes[hash]++
	mc.metrics.StrategyBreakdown[strategy]++

	if mc.hashes[hash] == 1 {
		mc.metrics.UniqueHashes++
	}
}

// GetMetrics returns the collected metrics.
func (mc *MetricsCollector) GetMetrics() HashingMetrics {
	// Calculate collision rate
	if mc.metrics.TotalRows > 0 {
		mc.metrics.CollisionRate = 1.0 - (float64(mc.metrics.UniqueHashes) / float64(mc.metrics.TotalRows))
	}

	// Create hash distribution summary
	mc.metrics.HashDistribution = make(map[string]int64)
	for hash, count := range mc.hashes {
		if count > 1 {
			mc.metrics.HashDistribution[hash] = count
		}
	}

	return mc.metrics
}

// HashingStrategy utilities.

// ValidateHashingStrategy validates a hashing strategy configuration.
func ValidateHashingStrategy(strategy config.HashingStrategy) error {
	switch strategy.Mode {
	case modeFullRow:
		// No additional validation needed
		return nil
	case modeSelective:
		if len(strategy.IncludeKeys) == 0 {
			return fmt.Errorf("selective hashing requires at least one include key")
		}
		return nil
	case modeExcludeKeys:
		if len(strategy.ExcludeKeys) == 0 {
			return fmt.Errorf("%s hashing requires at least one exclude key", modeExcludeKeys)
		}
		return nil
	default:
		return fmt.Errorf("unknown hashing mode: %s", strategy.Mode)
	}
}

// OptimizeHashingStrategy suggests optimizations for a given strategy.
func OptimizeHashingStrategy(strategy config.HashingStrategy, sampleData []report.JSONData) config.HashingStrategy {
	optimized := strategy

	if strategy.Mode == modeExcludeKeys {
		// Analyze which keys cause the most variance
		keyVariance := analyzeKeyVariance(sampleData)

		// Suggest excluding high-variance keys
		var highVarianceKeys []string
		for key, variance := range keyVariance {
			if variance > 0.8 { // 80% variance threshold
				highVarianceKeys = append(highVarianceKeys, key)
			}
		}

		if len(highVarianceKeys) > 0 {
			optimized.ExcludeKeys = append(optimized.ExcludeKeys, highVarianceKeys...)
		}
	}

	return optimized
}

// analyzeKeyVariance analyzes the variance of keys in sample data.
func analyzeKeyVariance(sampleData []report.JSONData) map[string]float64 {
	keyValues := make(map[string]map[string]int)
	keyTotals := make(map[string]int)

	// Count unique values per key
	for _, data := range sampleData {
		for key, value := range data {
			if keyValues[key] == nil {
				keyValues[key] = make(map[string]int)
			}

			valueStr := fmt.Sprintf("%v", value)
			keyValues[key][valueStr]++
			keyTotals[key]++
		}
	}

	// Calculate variance (unique values / total occurrences)
	variance := make(map[string]float64)
	for key, values := range keyValues {
		if keyTotals[key] > 0 {
			variance[key] = float64(len(values)) / float64(keyTotals[key])
		}
	}

	return variance
}
