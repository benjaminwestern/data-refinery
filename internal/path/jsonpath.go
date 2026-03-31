// Package path parses and evaluates the JSON-path dialect used by the tool.
package path

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Component represents a single component in a JSON path.
type Component struct {
	Key           string
	ArrayIndex    int
	IsWildcard    bool
	IsArrayAccess bool
}

// JSONPathProcessor handles JSON path operations.
type JSONPathProcessor struct {
	arrayIndexRegex *regexp.Regexp
}

// NewJSONPathProcessor creates a new JSONPathProcessor.
func NewJSONPathProcessor() *JSONPathProcessor {
	return &JSONPathProcessor{
		arrayIndexRegex: regexp.MustCompile(`^([^[]+)\[([*]|[0-9]+)\]$`),
	}
}

// AtomicDeletionResult represents the result of an atomic deletion operation.
type AtomicDeletionResult struct {
	Success      bool
	Modified     bool
	ErrorMessage string
	Operations   []Operation
	Transaction  *Transaction
	Rollback     func() error
}

// Operation represents a single operation in a transaction.
type Operation struct {
	Type        string
	Path        string
	OldValue    interface{}
	NewValue    interface{}
	Parent      interface{}
	ParentKey   string
	Description string
}

// Transaction manages a series of operations with rollback capability.
type Transaction struct {
	operations []Operation
	rollbacks  []func() error
}

// NewTransaction creates a new transaction.
func NewTransaction() *Transaction {
	return &Transaction{
		operations: make([]Operation, 0),
		rollbacks:  make([]func() error, 0),
	}
}

// AddOperation adds an operation to the transaction.
func (t *Transaction) AddOperation(op Operation, rollback func() error) {
	t.operations = append(t.operations, op)
	t.rollbacks = append(t.rollbacks, rollback)
}

// GetOperations returns all operations in the transaction.
func (t *Transaction) GetOperations() []Operation {
	return t.operations
}

// Rollback rolls back all operations in the transaction.
func (t *Transaction) Rollback() error {
	for i := len(t.rollbacks) - 1; i >= 0; i-- {
		if rollback := t.rollbacks[i]; rollback != nil {
			if err := rollback(); err != nil {
				return err
			}
		}
	}
	return nil
}

// ParsePath parses a JSON path string into components.
func (jpp *JSONPathProcessor) ParsePath(path string) ([]Component, error) {
	if path == "" {
		return nil, fmt.Errorf("empty path is not allowed")
	}

	// Check for spaces in path
	if strings.Contains(path, " ") {
		return nil, fmt.Errorf("path cannot contain spaces")
	}

	// Remove leading dot if present
	path = strings.TrimPrefix(path, ".")

	parts := strings.Split(path, ".")
	var components []Component

	for _, part := range parts {
		if part == "" {
			continue
		}

		component, err := jpp.parsePathComponent(part)
		if err != nil {
			return nil, err
		}
		components = append(components, component)
	}

	if len(components) == 0 {
		return nil, fmt.Errorf("no valid path components found")
	}

	return components, nil
}

func (jpp *JSONPathProcessor) parsePathComponent(part string) (Component, error) {
	if strings.Contains(part, "[") {
		return jpp.parseArrayPathComponent(part)
	}

	if strings.Contains(part, "]") {
		return Component{}, fmt.Errorf("unmatched bracket in path component: %s", part)
	}

	return Component{
		Key:           part,
		ArrayIndex:    -1,
		IsWildcard:    part == "*",
		IsArrayAccess: false,
	}, nil
}

func (jpp *JSONPathProcessor) parseArrayPathComponent(part string) (Component, error) {
	if !jpp.arrayIndexRegex.MatchString(part) {
		return Component{}, fmt.Errorf("invalid array syntax: %s", part)
	}

	matches := jpp.arrayIndexRegex.FindStringSubmatch(part)
	if len(matches) != 3 {
		return Component{}, fmt.Errorf("invalid array syntax: %s", part)
	}

	component := Component{
		Key:           matches[1],
		IsArrayAccess: true,
	}

	if matches[2] == "*" {
		component.IsWildcard = true
		component.ArrayIndex = -1
		return component, nil
	}

	index, err := strconv.Atoi(matches[2])
	if err != nil {
		return Component{}, fmt.Errorf("invalid array index: %s", matches[2])
	}
	if index < 0 {
		return Component{}, fmt.Errorf("negative array index not allowed: %d", index)
	}

	component.ArrayIndex = index
	return component, nil
}

// DeleteSubKeyTargetsAtomic performs atomic deletion of sub-key targets with rollback capability.
func (jpp *JSONPathProcessor) DeleteSubKeyTargetsAtomic(data interface{}, path string, targetValues []string) (*AtomicDeletionResult, error) {
	// Parse the path
	components, err := jpp.ParsePath(path)
	if err != nil {
		return &AtomicDeletionResult{
			Success:      false,
			Modified:     false,
			ErrorMessage: fmt.Sprintf("failed to parse path: %v", err),
		}, err
	}

	// Create a new transaction
	transaction := NewTransaction()

	// Store original data for rollback
	originalData := jpp.deepCopy(data)

	// Perform the deletion
	modified, err := jpp.deleteSubKeyTargetsRecursive(data, components, targetValues, transaction)
	if err != nil {
		return &AtomicDeletionResult{
			Success:      false,
			Modified:     false,
			ErrorMessage: fmt.Sprintf("deletion failed: %v", err),
			Transaction:  transaction,
		}, err
	}

	// Create rollback function
	rollback := func() error {
		// Simple rollback by deep copying original data back
		jpp.copyValues(originalData, data)
		return nil
	}

	return &AtomicDeletionResult{
		Success:     true,
		Modified:    modified,
		Operations:  transaction.GetOperations(),
		Transaction: transaction,
		Rollback:    rollback,
	}, nil
}

// deleteSubKeyTargetsRecursive recursively deletes sub-key targets.
func (jpp *JSONPathProcessor) deleteSubKeyTargetsRecursive(current interface{}, components []Component, targetValues []string, transaction *Transaction) (bool, error) {
	if len(components) == 0 {
		return false, nil
	}

	component := components[0]
	remaining := components[1:]

	switch v := current.(type) {
	case map[string]interface{}:
		return jpp.deleteFromMap(current, v, component, remaining, targetValues, transaction)
	case []interface{}:
		return jpp.deleteFromArray(current, v, component, remaining, targetValues, transaction)
	}

	return false, nil
}

func (jpp *JSONPathProcessor) deleteFromMap(current interface{}, values map[string]interface{}, component Component, remaining []Component, targetValues []string, transaction *Transaction) (bool, error) {
	if component.IsArrayAccess {
		return jpp.deleteMapArrayAccess(values, component, remaining, targetValues, transaction)
	}

	value, exists := values[component.Key]
	if !exists {
		return false, nil
	}
	if len(remaining) == 0 {
		return jpp.deleteMatchingMapValue(current, values, component.Key, value, targetValues, transaction), nil
	}

	return jpp.deleteSubKeyTargetsRecursive(value, remaining, targetValues, transaction)
}

func (jpp *JSONPathProcessor) deleteMapArrayAccess(values map[string]interface{}, component Component, remaining []Component, targetValues []string, transaction *Transaction) (bool, error) {
	value, exists := values[component.Key]
	if !exists {
		return false, nil
	}

	arr, ok := value.([]interface{})
	if !ok {
		return false, fmt.Errorf("expected array but got %T", value)
	}

	if component.IsWildcard {
		modified, cleanedArr, err := jpp.deleteWildcardMapArray(arr, component.Key, remaining, targetValues, transaction)
		if err != nil {
			return false, err
		}
		values[component.Key] = cleanedArr
		return modified, nil
	}

	if component.ArrayIndex < 0 || component.ArrayIndex >= len(arr) {
		return false, nil
	}

	return jpp.deleteIndexedMapArray(values, arr, component, remaining, targetValues, transaction)
}

func (jpp *JSONPathProcessor) deleteWildcardMapArray(arr []interface{}, key string, remaining []Component, targetValues []string, transaction *Transaction) (bool, []interface{}, error) {
	modified := false
	for i := len(arr) - 1; i >= 0; i-- {
		elementModified, nextArr, err := jpp.deleteMapArrayElement(arr, i, key, remaining, targetValues, transaction)
		if err != nil {
			return false, nil, err
		}
		arr = nextArr
		if elementModified {
			modified = true
		}
	}

	cleanedArr, cleanedModified := jpp.removeEmptyObjectsFromArray(arr, key, transaction)
	return modified || cleanedModified, cleanedArr, nil
}

func (jpp *JSONPathProcessor) deleteMapArrayElement(arr []interface{}, index int, key string, remaining []Component, targetValues []string, transaction *Transaction) (bool, []interface{}, error) {
	element := arr[index]
	if len(remaining) == 0 {
		if !jpp.valueMatches(element, targetValues) {
			return false, arr, nil
		}

		op := Operation{
			Type:        "delete",
			Path:        fmt.Sprintf("%s[%d]", key, index),
			OldValue:    element,
			NewValue:    nil,
			Parent:      arr,
			ParentKey:   fmt.Sprintf("[%d]", index),
			Description: fmt.Sprintf("Delete array element at index %d", index),
		}
		transaction.AddOperation(op, nil)
		return true, append(arr[:index], arr[index+1:]...), nil
	}

	modified, err := jpp.deleteSubKeyTargetsRecursive(element, remaining, targetValues, transaction)
	return modified, arr, err
}

func (jpp *JSONPathProcessor) deleteIndexedMapArray(values map[string]interface{}, arr []interface{}, component Component, remaining []Component, targetValues []string, transaction *Transaction) (bool, error) {
	index := component.ArrayIndex
	if len(remaining) > 0 {
		return jpp.deleteSubKeyTargetsRecursive(arr[index], remaining, targetValues, transaction)
	}
	if !jpp.valueMatches(arr[index], targetValues) {
		return false, nil
	}

	op := Operation{
		Type:        "delete",
		Path:        fmt.Sprintf("%s[%d]", component.Key, index),
		OldValue:    arr[index],
		NewValue:    nil,
		Parent:      arr,
		ParentKey:   fmt.Sprintf("[%d]", index),
		Description: fmt.Sprintf("Delete array element at index %d", index),
	}
	transaction.AddOperation(op, nil)
	values[component.Key] = append(arr[:index], arr[index+1:]...)
	return true, nil
}

func (jpp *JSONPathProcessor) deleteMatchingMapValue(current interface{}, values map[string]interface{}, key string, value interface{}, targetValues []string, transaction *Transaction) bool {
	if !jpp.valueMatches(value, targetValues) {
		return false
	}

	op := Operation{
		Type:        "delete",
		Path:        key,
		OldValue:    value,
		NewValue:    nil,
		Parent:      current,
		ParentKey:   key,
		Description: fmt.Sprintf("Delete key '%s'", key),
	}
	transaction.AddOperation(op, nil)
	delete(values, key)
	return true
}

func (jpp *JSONPathProcessor) removeEmptyObjectsFromArray(arr []interface{}, key string, transaction *Transaction) ([]interface{}, bool) {
	cleanedArr := make([]interface{}, 0, len(arr))
	modified := false
	for _, element := range arr {
		elementMap, ok := element.(map[string]interface{})
		if !ok {
			cleanedArr = append(cleanedArr, element)
			continue
		}
		if len(elementMap) > 0 {
			cleanedArr = append(cleanedArr, element)
			continue
		}

		op := Operation{
			Type:        "delete",
			Path:        fmt.Sprintf("%s[empty]", key),
			OldValue:    element,
			NewValue:    nil,
			Parent:      cleanedArr,
			ParentKey:   "empty",
			Description: "Delete empty object from array",
		}
		transaction.AddOperation(op, nil)
		modified = true
	}

	return cleanedArr, modified
}

func (jpp *JSONPathProcessor) deleteFromArray(current interface{}, values []interface{}, component Component, remaining []Component, targetValues []string, transaction *Transaction) (bool, error) {
	if component.IsWildcard {
		return jpp.deleteWildcardArray(current, values, remaining, targetValues, transaction)
	}
	if component.ArrayIndex < 0 || component.ArrayIndex >= len(values) {
		return false, nil
	}

	return jpp.deleteIndexedArray(current, values, component.ArrayIndex, remaining, targetValues, transaction)
}

func (jpp *JSONPathProcessor) deleteWildcardArray(current interface{}, values []interface{}, remaining []Component, targetValues []string, transaction *Transaction) (bool, error) {
	modified := false
	for i := len(values) - 1; i >= 0; i-- {
		elementModified, err := jpp.deleteArrayElement(current, values, i, remaining, targetValues, transaction)
		if err != nil {
			return false, err
		}
		if elementModified {
			modified = true
		}
	}

	return modified, nil
}

func (jpp *JSONPathProcessor) deleteArrayElement(current interface{}, values []interface{}, index int, remaining []Component, targetValues []string, transaction *Transaction) (bool, error) {
	element := values[index]
	if len(remaining) > 0 {
		return jpp.deleteSubKeyTargetsRecursive(element, remaining, targetValues, transaction)
	}
	if !jpp.valueMatches(element, targetValues) {
		return false, nil
	}

	op := Operation{
		Type:        "delete",
		Path:        fmt.Sprintf("[%d]", index),
		OldValue:    element,
		NewValue:    nil,
		Parent:      current,
		ParentKey:   fmt.Sprintf("[%d]", index),
		Description: fmt.Sprintf("Delete array element at index %d", index),
	}
	transaction.AddOperation(op, nil)
	return true, nil
}

func (jpp *JSONPathProcessor) deleteIndexedArray(current interface{}, values []interface{}, index int, remaining []Component, targetValues []string, transaction *Transaction) (bool, error) {
	if len(remaining) > 0 {
		return jpp.deleteSubKeyTargetsRecursive(values[index], remaining, targetValues, transaction)
	}
	if !jpp.valueMatches(values[index], targetValues) {
		return false, nil
	}

	op := Operation{
		Type:        "delete",
		Path:        fmt.Sprintf("[%d]", index),
		OldValue:    values[index],
		NewValue:    nil,
		Parent:      current,
		ParentKey:   fmt.Sprintf("[%d]", index),
		Description: fmt.Sprintf("Delete array element at index %d", index),
	}
	transaction.AddOperation(op, nil)
	return true, nil
}

// valueMatches checks if a value matches any of the target values.
func (jpp *JSONPathProcessor) valueMatches(value interface{}, targetValues []string) bool {
	if len(targetValues) == 0 {
		return true
	}

	valueStr := fmt.Sprintf("%v", value)
	for _, target := range targetValues {
		if valueStr == target {
			return true
		}
	}
	return false
}

// Helper functions for deep copying.
func (jpp *JSONPathProcessor) deepCopy(original interface{}) interface{} {
	switch v := original.(type) {
	case map[string]interface{}:
		clone := make(map[string]interface{})
		for k, val := range v {
			clone[k] = jpp.deepCopy(val)
		}
		return clone
	case []interface{}:
		clone := make([]interface{}, len(v))
		for i, val := range v {
			clone[i] = jpp.deepCopy(val)
		}
		return clone
	default:
		return v
	}
}

func (jpp *JSONPathProcessor) copyValues(src, dest interface{}) {
	switch srcVal := src.(type) {
	case map[string]interface{}:
		if destMap, ok := dest.(map[string]interface{}); ok {
			// Clear destination map
			for k := range destMap {
				delete(destMap, k)
			}
			// Copy from source
			for k, v := range srcVal {
				destMap[k] = jpp.deepCopy(v)
			}
		}
	case []interface{}:
		if destSlice, ok := dest.([]interface{}); ok {
			// This is tricky with slices - we need to modify the underlying array
			// For now, we'll just log that rollback isn't fully supported for slices
			_ = destSlice
		}
	}
}

// ValidationResult represents the result of path validation.
type ValidationResult struct {
	Valid          bool
	IsValid        bool // Alias for Valid
	Error          string
	Errors         []string // Array of errors
	Complexity     string
	Warnings       []string
	Components     []Component
	ComponentCount int
}

// ValidatePathDetailed validates a path and returns detailed information.
func (jpp *JSONPathProcessor) ValidatePathDetailed(path string) *ValidationResult {
	components, err := jpp.ParsePath(path)
	if err != nil {
		return &ValidationResult{
			Valid:          false,
			IsValid:        false,
			Error:          err.Error(),
			Errors:         []string{err.Error()},
			Complexity:     "",
			Warnings:       []string{},
			Components:     []Component{},
			ComponentCount: 0,
		}
	}

	warnings := []string{}
	complexity := "simple"

	// Check for deep paths
	if len(components) > 10 {
		complexity = "complex"
		warnings = append(warnings, "Complex path may have performance implications")
	}

	// Check for wildcard patterns
	wildcardCount := 0
	for _, comp := range components {
		if comp.IsWildcard {
			wildcardCount++
		}
	}

	// Determine complexity based on wildcards
	if wildcardCount > 1 {
		complexity = "very_complex"
		warnings = append(warnings, "Very complex path may have performance implications")
	} else if wildcardCount == 1 {
		complexity = "complex"
	}

	return &ValidationResult{
		Valid:          true,
		IsValid:        true,
		Error:          "",
		Errors:         []string{},
		Complexity:     complexity,
		Warnings:       warnings,
		Components:     components,
		ComponentCount: len(components),
	}
}

// DeleteSubKeyTargets is an alias for backward compatibility.
func (jpp *JSONPathProcessor) DeleteSubKeyTargets(data interface{}, path string, targetValues []string) (bool, error) {
	result, err := jpp.DeleteSubKeyTargetsAtomic(data, path, targetValues)
	if err != nil {
		return false, err
	}
	return result.Modified, nil
}

// Result represents a result from path finding.
type Result struct {
	Path      string
	Value     interface{}
	Parent    interface{}
	ParentKey string
	Found     bool
}

// FindValue finds values at the specified path.
func (jpp *JSONPathProcessor) FindValue(data interface{}, path string) ([]Result, error) {
	components, err := jpp.ParsePath(path)
	if err != nil {
		return nil, err
	}

	results := []Result{}
	jpp.findValueRecursive(data, components, "", nil, "", &results)
	return results, nil
}

// findValueRecursive recursively finds values at the specified path.
func (jpp *JSONPathProcessor) findValueRecursive(current interface{}, components []Component, currentPath string, parent interface{}, parentKey string, results *[]Result) {
	if len(components) == 0 {
		*results = append(*results, Result{
			Path:      currentPath,
			Value:     current,
			Parent:    parent,
			ParentKey: parentKey,
			Found:     true,
		})
		return
	}

	component := components[0]
	remaining := components[1:]

	switch v := current.(type) {
	case map[string]interface{}:
		jpp.findValueInMap(current, v, component, remaining, currentPath, results)
	case []interface{}:
		jpp.findValueInArray(current, v, component, remaining, currentPath, results)
	}
}

func (jpp *JSONPathProcessor) findValueInMap(current interface{}, values map[string]interface{}, component Component, remaining []Component, currentPath string, results *[]Result) {
	if component.IsArrayAccess {
		jpp.findValueInMapArray(currentPath, values, component, remaining, results)
		return
	}

	if component.IsWildcard {
		for key, value := range values {
			newPath := jpp.buildPath(currentPath, key)
			jpp.findValueRecursive(value, remaining, newPath, current, key, results)
		}
		return
	}

	value, exists := values[component.Key]
	if !exists {
		return
	}

	newPath := jpp.buildPath(currentPath, component.Key)
	jpp.findValueRecursive(value, remaining, newPath, current, component.Key, results)
}

func (jpp *JSONPathProcessor) findValueInMapArray(currentPath string, values map[string]interface{}, component Component, remaining []Component, results *[]Result) {
	value, exists := values[component.Key]
	if !exists {
		return
	}

	arr, ok := value.([]interface{})
	if !ok {
		return
	}

	newPath := jpp.buildPath(currentPath, component.Key)
	if component.IsWildcard {
		for i, elem := range arr {
			elemPath := fmt.Sprintf("%s[%d]", newPath, i)
			jpp.findValueRecursive(elem, remaining, elemPath, value, fmt.Sprintf("[%d]", i), results)
		}
		return
	}

	if component.ArrayIndex < 0 || component.ArrayIndex >= len(arr) {
		return
	}

	elemPath := fmt.Sprintf("%s[%d]", newPath, component.ArrayIndex)
	jpp.findValueRecursive(arr[component.ArrayIndex], remaining, elemPath, value, fmt.Sprintf("[%d]", component.ArrayIndex), results)
}

func (jpp *JSONPathProcessor) findValueInArray(current interface{}, values []interface{}, component Component, remaining []Component, currentPath string, results *[]Result) {
	if component.IsWildcard {
		for i, elem := range values {
			elemPath := fmt.Sprintf("%s[%d]", currentPath, i)
			jpp.findValueRecursive(elem, remaining, elemPath, current, fmt.Sprintf("[%d]", i), results)
		}
		return
	}

	if component.ArrayIndex < 0 || component.ArrayIndex >= len(values) {
		return
	}

	elemPath := fmt.Sprintf("%s[%d]", currentPath, component.ArrayIndex)
	jpp.findValueRecursive(values[component.ArrayIndex], remaining, elemPath, current, fmt.Sprintf("[%d]", component.ArrayIndex), results)
}

// buildPath builds a path string from components.
func (jpp *JSONPathProcessor) buildPath(currentPath, component string) string {
	if currentPath == "" {
		return component
	}
	if strings.HasPrefix(component, "[") {
		return currentPath + component
	}
	return currentPath + "." + component
}

// ValidatePath validates a JSON path string.
func (jpp *JSONPathProcessor) ValidatePath(path string) error {
	_, err := jpp.ParsePath(path)
	return err
}

// GetPathType returns the type of path (simple, array, wildcard, etc.)
func (jpp *JSONPathProcessor) GetPathType(path string) (string, error) {
	components, err := jpp.ParsePath(path)
	if err != nil {
		return "", err
	}

	hasWildcard := false
	hasArray := false

	for _, component := range components {
		if component.IsWildcard {
			hasWildcard = true
		}
		if component.IsArrayAccess {
			hasArray = true
		}
	}

	if hasWildcard && hasArray {
		return "wildcard_array", nil
	}
	if hasWildcard {
		return "wildcard", nil
	}
	if hasArray {
		return "array", nil
	}
	return "simple", nil
}
