// Package search evaluates configured duplicate and match targets against JSON rows.
package search

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/report"
)

// Engine manages multiple search targets and their matchers.
type Engine struct {
	targets       map[string]config.SearchTarget
	matchers      map[string]Matcher
	results       map[string][]MatchResult
	processedRows int64
	mutex         sync.RWMutex
}

// Matcher interface for different search implementations.
type Matcher interface {
	Match(data report.JSONData, target config.SearchTarget) []MatchResult
}

// MatchResult represents a single match found in the data.
type MatchResult struct {
	Found        bool
	Path         string
	Value        any
	Location     report.LocationInfo
	MatchedValue string
	Target       string
}

// Results contains all matches organized by target.
type Results struct {
	Results map[string][]MatchResult `json:"results"`
	Summary Summary                  `json:"summary"`
}

// Summary provides aggregate statistics.
type Summary struct {
	TotalMatches    int            `json:"totalMatches"`
	MatchesByTarget map[string]int `json:"matchesByTarget"`
	ProcessedRows   int64          `json:"processedRows"`
}

// NewSearchEngine creates a new search engine with configured targets.
func NewSearchEngine(targets []config.SearchTarget) (*Engine, error) {
	engine := &Engine{
		targets:  make(map[string]config.SearchTarget),
		matchers: make(map[string]Matcher),
		results:  make(map[string][]MatchResult),
	}

	// Initialize targets and matchers
	for _, target := range targets {
		engine.targets[target.Name] = target

		matcher, err := createMatcher(target.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to create matcher for target %s: %w", target.Name, err)
		}
		engine.matchers[target.Name] = matcher
		engine.results[target.Name] = make([]MatchResult, 0)
	}

	return engine, nil
}

// createMatcher creates the appropriate matcher for a given type.
func createMatcher(matcherType string) (Matcher, error) {
	switch matcherType {
	case "direct":
		return &directMatcher{}, nil
	case "nested_array":
		return &nestedArrayMatcher{}, nil
	case "nested_object":
		return &nestedObjectMatcher{}, nil
	case "jsonpath":
		return &jsonPathMatcher{}, nil
	default:
		return nil, fmt.Errorf("unknown matcher type: %s", matcherType)
	}
}

// Search processes a row of data against all configured targets.
func (se *Engine) Search(data report.JSONData, location report.LocationInfo) {
	matchesByTarget := se.MatchRow(data, location)

	se.mutex.Lock()
	defer se.mutex.Unlock()

	se.processedRows++
	for targetName, matches := range matchesByTarget {
		se.results[targetName] = append(se.results[targetName], matches...)
	}
}

// MatchRow returns matches for a single row without mutating accumulated results.
func (se *Engine) MatchRow(data report.JSONData, location report.LocationInfo) map[string][]MatchResult {
	se.mutex.RLock()
	defer se.mutex.RUnlock()

	resultsByTarget := make(map[string][]MatchResult, len(se.targets))
	for targetName, target := range se.targets {
		matcher := se.matchers[targetName]
		matches := matcher.Match(data, target)

		foundMatches := make([]MatchResult, 0, len(matches))
		for i := range matches {
			matches[i].Location = location
			matches[i].Target = targetName
			if matches[i].Found {
				foundMatches = append(foundMatches, matches[i])
			}
		}

		resultsByTarget[targetName] = foundMatches
	}

	return resultsByTarget
}

// GetResults returns all search results.
func (se *Engine) GetResults() Results {
	se.mutex.RLock()
	defer se.mutex.RUnlock()

	totalMatches := 0
	matchesByTarget := make(map[string]int)

	for targetName, matches := range se.results {
		matchCount := len(matches)
		totalMatches += matchCount
		matchesByTarget[targetName] = matchCount
	}

	return Results{
		Results: se.results,
		Summary: Summary{
			TotalMatches:    totalMatches,
			MatchesByTarget: matchesByTarget,
			ProcessedRows:   se.processedRows,
		},
	}
}

// GetResultsForTarget returns results for a specific target.
func (se *Engine) GetResultsForTarget(targetName string) []MatchResult {
	se.mutex.RLock()
	defer se.mutex.RUnlock()

	if results, exists := se.results[targetName]; exists {
		return results
	}
	return nil
}

// ClearResults clears all search results.
func (se *Engine) ClearResults() {
	se.mutex.Lock()
	defer se.mutex.Unlock()

	for targetName := range se.results {
		se.results[targetName] = make([]MatchResult, 0)
	}
	se.processedRows = 0
}

// directMatcher implements simple key-value matching.
type directMatcher struct{}

func (dm *directMatcher) Match(data report.JSONData, target config.SearchTarget) []MatchResult {
	var results []MatchResult

	value, exists := data[target.Path]
	if !exists {
		return results
	}

	valueStr := fmt.Sprintf("%v", value)

	for _, targetValue := range target.TargetValues {
		if dm.matchValue(valueStr, targetValue, target.CaseSensitive) {
			results = append(results, MatchResult{
				Found:        true,
				Path:         target.Path,
				Value:        value,
				MatchedValue: valueStr,
			})
		}
	}

	return results
}

func (dm *directMatcher) matchValue(actual, target string, caseSensitive bool) bool {
	if !caseSensitive {
		actual = strings.ToLower(actual)
		target = strings.ToLower(target)
	}
	return actual == target
}

// nestedArrayMatcher implements searching within arrays.
type nestedArrayMatcher struct{}

func (nam *nestedArrayMatcher) Match(data report.JSONData, target config.SearchTarget) []MatchResult {
	var results []MatchResult

	// Parse path like "line-items[*].line-item-id"
	pathParts := strings.Split(target.Path, ".")
	if len(pathParts) < 2 {
		return results
	}

	// Get the array field
	arrayField := pathParts[0]
	arrayField = strings.TrimSuffix(arrayField, "[*]")

	arrayValue, exists := data[arrayField]
	if !exists {
		return results
	}

	// Convert to array
	arrayData, ok := arrayValue.([]any)
	if !ok {
		return results
	}

	// Get the nested field path
	nestedPath := strings.Join(pathParts[1:], ".")

	// Search within each array element
	for i, element := range arrayData {
		if elementObj, ok := element.(map[string]any); ok {
			elementMatches := nam.searchInObject(elementObj, nestedPath, target.TargetValues, target.CaseSensitive)
			for _, match := range elementMatches {
				match.Path = fmt.Sprintf("%s[%d].%s", arrayField, i, match.Path)
				results = append(results, match)
			}
		}
	}

	return results
}

func (nam *nestedArrayMatcher) searchInObject(obj map[string]any, path string, targetValues []string, caseSensitive bool) []MatchResult {
	var results []MatchResult

	value, exists := obj[path]
	if !exists {
		return results
	}

	valueStr := fmt.Sprintf("%v", value)

	for _, targetValue := range targetValues {
		if nam.matchValue(valueStr, targetValue, caseSensitive) {
			results = append(results, MatchResult{
				Found:        true,
				Path:         path,
				Value:        value,
				MatchedValue: valueStr,
			})
		}
	}

	return results
}

func (nam *nestedArrayMatcher) matchValue(actual, target string, caseSensitive bool) bool {
	if !caseSensitive {
		actual = strings.ToLower(actual)
		target = strings.ToLower(target)
	}
	return actual == target
}

// nestedObjectMatcher implements deep object traversal.
type nestedObjectMatcher struct{}

func (nom *nestedObjectMatcher) Match(data report.JSONData, target config.SearchTarget) []MatchResult {
	var results []MatchResult

	// Navigate to the nested object using dot notation
	current := any(data)
	pathParts := strings.Split(target.Path, ".")

	for i, part := range pathParts {
		var currentMap map[string]any
		var ok bool

		// Try to cast to map[string]any or report.JSONData
		if currentMap, ok = current.(map[string]any); !ok {
			if jsonData, ok := current.(report.JSONData); ok {
				currentMap = jsonData
			} else {
				// Not a map, can't traverse further
				break
			}
		}

		if val, exists := currentMap[part]; exists {
			current = val

			// If this is the last part, check for matches
			if i == len(pathParts)-1 {
				valueStr := fmt.Sprintf("%v", val)

				for _, targetValue := range target.TargetValues {
					if nom.matchValue(valueStr, targetValue, target.CaseSensitive) {
						results = append(results, MatchResult{
							Found:        true,
							Path:         target.Path,
							Value:        val,
							MatchedValue: valueStr,
						})
					}
				}
			}
		} else {
			// Path doesn't exist
			break
		}
	}

	return results
}

func (nom *nestedObjectMatcher) matchValue(actual, target string, caseSensitive bool) bool {
	if !caseSensitive {
		actual = strings.ToLower(actual)
		target = strings.ToLower(target)
	}
	return actual == target
}

// jsonPathMatcher implements JSONPath-like queries (simplified).
type jsonPathMatcher struct{}

func (jpm *jsonPathMatcher) Match(data report.JSONData, target config.SearchTarget) []MatchResult {
	var results []MatchResult

	// This is a simplified JSONPath implementation
	// For production use, consider using a full JSONPath library
	paths := jpm.evaluateJSONPath(data, target.Path)

	for _, pathResult := range paths {
		valueStr := fmt.Sprintf("%v", pathResult.Value)

		for _, targetValue := range target.TargetValues {
			if jpm.matchValue(valueStr, targetValue, target.CaseSensitive) {
				results = append(results, MatchResult{
					Found:        true,
					Path:         pathResult.Path,
					Value:        pathResult.Value,
					MatchedValue: valueStr,
				})
			}
		}
	}

	return results
}

type pathResult struct {
	Path  string
	Value any
}

func (jpm *jsonPathMatcher) evaluateJSONPath(data report.JSONData, jsonPath string) []pathResult {
	// Simple JSONPath evaluation - supports basic dot notation and wildcards
	if strings.Contains(jsonPath, "*") {
		return jpm.evaluateWildcardPath(data, jsonPath, "")
	}

	return jpm.evaluateSimplePath(data, jsonPath)
}

func (jpm *jsonPathMatcher) evaluateSimplePath(data report.JSONData, path string) []pathResult {
	results := make([]pathResult, 0, 1)

	parts := strings.Split(path, ".")
	current := any(data)

	for _, part := range parts {
		var currentMap map[string]any
		var ok bool

		// Try to cast to map[string]any or report.JSONData
		if currentMap, ok = current.(map[string]any); !ok {
			if jsonData, ok := current.(report.JSONData); ok {
				currentMap = jsonData
			} else {
				return results // Not a map
			}
		}

		if val, exists := currentMap[part]; exists {
			current = val
		} else {
			return results // Path doesn't exist
		}
	}

	results = append(results, pathResult{
		Path:  path,
		Value: current,
	})

	return results
}

func (jpm *jsonPathMatcher) evaluateWildcardPath(data report.JSONData, path string, prefix string) []pathResult {
	var results []pathResult

	// This is a simplified wildcard implementation
	// In a full implementation, you'd want to handle more complex JSONPath expressions
	if strings.HasPrefix(path, "*.") {
		// Handle root-level wildcard
		remainingPath := path[2:]
		for key, value := range data {
			newPrefix := key
			if prefix != "" {
				newPrefix = prefix + "." + key
			}

			if valueMap, ok := value.(map[string]any); ok {
				subResults := jpm.evaluateWildcardPath(valueMap, remainingPath, newPrefix)
				results = append(results, subResults...)
			} else if jsonData, ok := value.(report.JSONData); ok {
				subResults := jpm.evaluateWildcardPath(jsonData, remainingPath, newPrefix)
				results = append(results, subResults...)
			}
		}
	} else {
		// Handle specific path with wildcards
		parts := strings.Split(path, ".")
		results = jpm.evaluatePathParts(data, parts, prefix, 0)
	}

	return results
}

func (jpm *jsonPathMatcher) evaluatePathParts(data any, parts []string, prefix string, index int) []pathResult {
	if index >= len(parts) {
		return nil
	}

	dataMap, ok := pathDataMap(data)
	if !ok {
		return nil
	}

	part := parts[index]
	if part == "*" {
		return jpm.evaluateWildcardPathParts(dataMap, parts, prefix, index)
	}

	return jpm.evaluateSpecificPathPart(dataMap, part, parts, prefix, index)
}

func pathDataMap(data any) (map[string]any, bool) {
	if dataMap, ok := data.(map[string]any); ok {
		return dataMap, true
	}

	jsonData, ok := data.(report.JSONData)
	if !ok {
		return nil, false
	}

	return jsonData, true
}

func (jpm *jsonPathMatcher) evaluateWildcardPathParts(dataMap map[string]any, parts []string, prefix string, index int) []pathResult {
	results := make([]pathResult, 0, len(dataMap))
	for key, value := range dataMap {
		results = append(results, jpm.evaluateNextPathPart(key, value, parts, prefix, index)...)
	}
	return results
}

func (jpm *jsonPathMatcher) evaluateSpecificPathPart(dataMap map[string]any, part string, parts []string, prefix string, index int) []pathResult {
	value, exists := dataMap[part]
	if !exists {
		return nil
	}

	return jpm.evaluateNextPathPart(part, value, parts, prefix, index)
}

func (jpm *jsonPathMatcher) evaluateNextPathPart(key string, value any, parts []string, prefix string, index int) []pathResult {
	newPrefix := key
	if prefix != "" {
		newPrefix = prefix + "." + key
	}

	if index == len(parts)-1 {
		return []pathResult{{
			Path:  newPrefix,
			Value: value,
		}}
	}

	return jpm.evaluatePathParts(value, parts, newPrefix, index+1)
}

func (jpm *jsonPathMatcher) matchValue(actual, target string, caseSensitive bool) bool {
	if !caseSensitive {
		actual = strings.ToLower(actual)
		target = strings.ToLower(target)
	}
	return actual == target
}

// UtilityMatcher provides additional utility functions.
type UtilityMatcher struct{}

// MatchRegex performs regex matching.
func (um *UtilityMatcher) MatchRegex(value string, pattern string, caseSensitive bool) (bool, error) {
	flags := ""
	if !caseSensitive {
		flags = "(?i)"
	}

	regex, err := regexp.Compile(flags + pattern)
	if err != nil {
		return false, fmt.Errorf("compile regex pattern %q: %w", pattern, err)
	}

	return regex.MatchString(value), nil
}

// MatchContains performs substring matching.
func (um *UtilityMatcher) MatchContains(value string, substring string, caseSensitive bool) bool {
	if !caseSensitive {
		value = strings.ToLower(value)
		substring = strings.ToLower(substring)
	}

	return strings.Contains(value, substring)
}

// MatchType checks if a value is of a specific type.
func (um *UtilityMatcher) MatchType(value any, expectedType string) bool {
	actualType := reflect.TypeOf(value)
	if actualType == nil {
		return expectedType == "nil"
	}

	switch expectedType {
	case "string":
		return actualType.Kind() == reflect.String
	case "int", "integer":
		return actualType.Kind() >= reflect.Int && actualType.Kind() <= reflect.Int64
	case "float", "number":
		return actualType.Kind() == reflect.Float32 || actualType.Kind() == reflect.Float64
	case "bool", "boolean":
		return actualType.Kind() == reflect.Bool
	case "array", "slice":
		return actualType.Kind() == reflect.Slice || actualType.Kind() == reflect.Array
	case "map", "object":
		return actualType.Kind() == reflect.Map
	default:
		return false
	}
}
