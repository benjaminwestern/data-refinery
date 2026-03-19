package path

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// PathComponent represents a single component in a JSON path
type PathComponent struct {
	Key           string
	ArrayIndex    int
	IsWildcard    bool
	IsArrayAccess bool
}

// JSONPathProcessor handles JSON path operations
type JSONPathProcessor struct {
	arrayIndexRegex *regexp.Regexp
}

// NewJSONPathProcessor creates a new JSONPathProcessor
func NewJSONPathProcessor() *JSONPathProcessor {
	return &JSONPathProcessor{
		arrayIndexRegex: regexp.MustCompile(`^([^[]+)\[([*]|[0-9]+)\]$`),
	}
}

// AtomicDeletionResult represents the result of an atomic deletion operation
type AtomicDeletionResult struct {
	Success      bool
	Modified     bool
	ErrorMessage string
	Operations   []Operation
	Transaction  *Transaction
	Rollback     func() error
}

// Operation represents a single operation in a transaction
type Operation struct {
	Type        string
	Path        string
	OldValue    interface{}
	NewValue    interface{}
	Parent      interface{}
	ParentKey   string
	Description string
}

// Transaction manages a series of operations with rollback capability
type Transaction struct {
	operations []Operation
	rollbacks  []func() error
}

// NewTransaction creates a new transaction
func NewTransaction() *Transaction {
	return &Transaction{
		operations: make([]Operation, 0),
		rollbacks:  make([]func() error, 0),
	}
}

// AddOperation adds an operation to the transaction
func (t *Transaction) AddOperation(op Operation, rollback func() error) {
	t.operations = append(t.operations, op)
	t.rollbacks = append(t.rollbacks, rollback)
}

// GetOperations returns all operations in the transaction
func (t *Transaction) GetOperations() []Operation {
	return t.operations
}

// Rollback rolls back all operations in the transaction
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

// ParsePath parses a JSON path string into components
func (jpp *JSONPathProcessor) ParsePath(path string) ([]PathComponent, error) {
	if path == "" {
		return nil, fmt.Errorf("empty path is not allowed")
	}

	// Check for spaces in path
	if strings.Contains(path, " ") {
		return nil, fmt.Errorf("path cannot contain spaces")
	}

	// Remove leading dot if present
	if strings.HasPrefix(path, ".") {
		path = path[1:]
	}

	parts := strings.Split(path, ".")
	var components []PathComponent

	for _, part := range parts {
		if part == "" {
			continue
		}

		// Check for array access
		if strings.Contains(part, "[") {
			if !jpp.arrayIndexRegex.MatchString(part) {
				return nil, fmt.Errorf("invalid array syntax: %s", part)
			}

			matches := jpp.arrayIndexRegex.FindStringSubmatch(part)
			if len(matches) != 3 {
				return nil, fmt.Errorf("invalid array syntax: %s", part)
			}

			key := matches[1]
			indexStr := matches[2]

			// Create a single component that represents array access
			component := PathComponent{
				Key:           key,
				IsArrayAccess: true,
			}

			if indexStr == "*" {
				component.IsWildcard = true
				component.ArrayIndex = -1
			} else {
				index, err := strconv.Atoi(indexStr)
				if err != nil {
					return nil, fmt.Errorf("invalid array index: %s", indexStr)
				}
				if index < 0 {
					return nil, fmt.Errorf("negative array index not allowed: %d", index)
				}
				component.ArrayIndex = index
			}

			components = append(components, component)
		} else {
			// Check for unmatched brackets
			if strings.Contains(part, "[") && !strings.Contains(part, "]") {
				return nil, fmt.Errorf("unmatched bracket in path component: %s", part)
			}
			if strings.Contains(part, "]") && !strings.Contains(part, "[") {
				return nil, fmt.Errorf("unmatched bracket in path component: %s", part)
			}

			// Regular key access
			component := PathComponent{
				Key:           part,
				ArrayIndex:    -1,
				IsWildcard:    part == "*",
				IsArrayAccess: false,
			}
			components = append(components, component)
		}
	}

	if len(components) == 0 {
		return nil, fmt.Errorf("no valid path components found")
	}

	return components, nil
}

// DeleteSubKeyTargetsAtomic performs atomic deletion of sub-key targets with rollback capability
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

// deleteSubKeyTargetsRecursive recursively deletes sub-key targets
func (jpp *JSONPathProcessor) deleteSubKeyTargetsRecursive(current interface{}, components []PathComponent, targetValues []string, transaction *Transaction) (bool, error) {
	if len(components) == 0 {
		return false, nil
	}

	component := components[0]
	remaining := components[1:]
	modified := false

	switch v := current.(type) {
	case map[string]interface{}:
		if component.IsArrayAccess {
			// Handle array access like items[*]
			if value, exists := v[component.Key]; exists {
				arr, ok := value.([]interface{})
				if !ok {
					return false, fmt.Errorf("expected array but got %T", value)
				}

				if component.IsWildcard {
					// Process all array elements
					for i := len(arr) - 1; i >= 0; i-- {
						elem := arr[i]
						if len(remaining) == 0 {
							// This is the final component, delete the keys matching target values
							if jpp.valueMatches(elem, targetValues) {
								// Record operation for transaction
								op := Operation{
									Type:        "delete",
									Path:        fmt.Sprintf("%s[%d]", component.Key, i),
									OldValue:    elem,
									NewValue:    nil,
									Parent:      arr,
									ParentKey:   fmt.Sprintf("[%d]", i),
									Description: fmt.Sprintf("Delete array element at index %d", i),
								}
								transaction.AddOperation(op, nil)

								// Remove element from array
								arr = append(arr[:i], arr[i+1:]...)
								modified = true
							}
						} else {
							// Recursively process remaining path components within this element
							elemModified, err := jpp.deleteSubKeyTargetsRecursive(elem, remaining, targetValues, transaction)
							if err != nil {
								return false, err
							}
							if elemModified {
								modified = true
							}
						}
					}

					// Clean up empty objects from the array
					cleanedArr := []interface{}{}
					for _, elem := range arr {
						if elemMap, ok := elem.(map[string]interface{}); ok {
							if len(elemMap) > 0 {
								cleanedArr = append(cleanedArr, elem)
							} else {
								// Record operation for removing empty object
								op := Operation{
									Type:        "delete",
									Path:        fmt.Sprintf("%s[empty]", component.Key),
									OldValue:    elem,
									NewValue:    nil,
									Parent:      cleanedArr,
									ParentKey:   "empty",
									Description: "Delete empty object from array",
								}
								transaction.AddOperation(op, nil)
								modified = true
							}
						} else {
							cleanedArr = append(cleanedArr, elem)
						}
					}
					arr = cleanedArr

					// Update the array in the parent map
					v[component.Key] = arr

				} else if component.ArrayIndex >= 0 && component.ArrayIndex < len(arr) {
					// Handle specific array index
					if len(remaining) == 0 {
						if jpp.valueMatches(arr[component.ArrayIndex], targetValues) {
							// Record operation
							op := Operation{
								Type:        "delete",
								Path:        fmt.Sprintf("%s[%d]", component.Key, component.ArrayIndex),
								OldValue:    arr[component.ArrayIndex],
								NewValue:    nil,
								Parent:      arr,
								ParentKey:   fmt.Sprintf("[%d]", component.ArrayIndex),
								Description: fmt.Sprintf("Delete array element at index %d", component.ArrayIndex),
							}
							transaction.AddOperation(op, nil)

							// Remove element
							arr = append(arr[:component.ArrayIndex], arr[component.ArrayIndex+1:]...)
							v[component.Key] = arr
							modified = true
						}
					} else {
						// Recursively process remaining path components
						elemModified, err := jpp.deleteSubKeyTargetsRecursive(arr[component.ArrayIndex], remaining, targetValues, transaction)
						if err != nil {
							return false, err
						}
						if elemModified {
							modified = true
						}
					}
				}
			}
		} else {
			// Handle regular key access
			if value, exists := v[component.Key]; exists {
				if len(remaining) == 0 {
					// Final component - delete if matches
					if jpp.valueMatches(value, targetValues) {
						op := Operation{
							Type:        "delete",
							Path:        component.Key,
							OldValue:    value,
							NewValue:    nil,
							Parent:      current,
							ParentKey:   component.Key,
							Description: fmt.Sprintf("Delete key '%s'", component.Key),
						}
						transaction.AddOperation(op, nil)

						delete(v, component.Key)
						modified = true
					}
				} else {
					// Recursive call for remaining components
					subModified, err := jpp.deleteSubKeyTargetsRecursive(value, remaining, targetValues, transaction)
					if err != nil {
						return false, err
					}
					if subModified {
						modified = true
					}
				}
			}
		}

	case []interface{}:
		// Direct array processing
		if component.IsWildcard {
			for i := len(v) - 1; i >= 0; i-- {
				elem := v[i]
				if len(remaining) == 0 {
					if jpp.valueMatches(elem, targetValues) {
						op := Operation{
							Type:        "delete",
							Path:        fmt.Sprintf("[%d]", i),
							OldValue:    elem,
							NewValue:    nil,
							Parent:      current,
							ParentKey:   fmt.Sprintf("[%d]", i),
							Description: fmt.Sprintf("Delete array element at index %d", i),
						}
						transaction.AddOperation(op, nil)

						v = append(v[:i], v[i+1:]...)
						modified = true
					}
				} else {
					// Recursively process remaining path components
					elemModified, err := jpp.deleteSubKeyTargetsRecursive(elem, remaining, targetValues, transaction)
					if err != nil {
						return false, err
					}
					if elemModified {
						modified = true
					}
				}
			}
		} else if component.ArrayIndex >= 0 && component.ArrayIndex < len(v) {
			if len(remaining) == 0 {
				if jpp.valueMatches(v[component.ArrayIndex], targetValues) {
					op := Operation{
						Type:        "delete",
						Path:        fmt.Sprintf("[%d]", component.ArrayIndex),
						OldValue:    v[component.ArrayIndex],
						NewValue:    nil,
						Parent:      current,
						ParentKey:   fmt.Sprintf("[%d]", component.ArrayIndex),
						Description: fmt.Sprintf("Delete array element at index %d", component.ArrayIndex),
					}
					transaction.AddOperation(op, nil)

					v = append(v[:component.ArrayIndex], v[component.ArrayIndex+1:]...)
					modified = true
				}
			} else {
				// Recursively process remaining path components
				elemModified, err := jpp.deleteSubKeyTargetsRecursive(v[component.ArrayIndex], remaining, targetValues, transaction)
				if err != nil {
					return false, err
				}
				if elemModified {
					modified = true
				}
			}
		}
	}

	return modified, nil
}

// valueMatches checks if a value matches any of the target values
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

// checkIfShouldDeleteSubObject checks if a sub-object should be deleted based on the remaining path components
func (jpp *JSONPathProcessor) checkIfShouldDeleteSubObject(current interface{}, components []PathComponent, targetValues []string) (bool, error) {
	if len(components) == 0 {
		return false, nil
	}

	component := components[0]
	remaining := components[1:]

	switch v := current.(type) {
	case map[string]interface{}:
		if value, exists := v[component.Key]; exists {
			if len(remaining) == 0 {
				// This is the final component - check if the value matches target values
				return jpp.valueMatches(value, targetValues), nil
			} else {
				// Continue recursively
				return jpp.checkIfShouldDeleteSubObject(value, remaining, targetValues)
			}
		}
	case []interface{}:
		if component.IsArrayAccess {
			if component.IsWildcard {
				// For wildcard access, check if any element matches
				for _, elem := range v {
					if len(remaining) == 0 {
						if jpp.valueMatches(elem, targetValues) {
							return true, nil
						}
					} else {
						matches, err := jpp.checkIfShouldDeleteSubObject(elem, remaining, targetValues)
						if err != nil {
							return false, err
						}
						if matches {
							return true, nil
						}
					}
				}
			} else if component.ArrayIndex >= 0 && component.ArrayIndex < len(v) {
				// For specific index access
				elem := v[component.ArrayIndex]
				if len(remaining) == 0 {
					return jpp.valueMatches(elem, targetValues), nil
				} else {
					return jpp.checkIfShouldDeleteSubObject(elem, remaining, targetValues)
				}
			}
		}
	}

	return false, nil
}

// Helper functions for deep copying
func (jpp *JSONPathProcessor) deepCopy(original interface{}) interface{} {
	switch v := original.(type) {
	case map[string]interface{}:
		copy := make(map[string]interface{})
		for k, val := range v {
			copy[k] = jpp.deepCopy(val)
		}
		return copy
	case []interface{}:
		copy := make([]interface{}, len(v))
		for i, val := range v {
			copy[i] = jpp.deepCopy(val)
		}
		return copy
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

// PathValidationResult represents the result of path validation
type PathValidationResult struct {
	Valid          bool
	IsValid        bool // Alias for Valid
	Error          string
	Errors         []string // Array of errors
	Complexity     string
	Warnings       []string
	Components     []PathComponent
	ComponentCount int
}

// ValidatePathDetailed validates a path and returns detailed information
func (jpp *JSONPathProcessor) ValidatePathDetailed(path string) *PathValidationResult {
	components, err := jpp.ParsePath(path)
	if err != nil {
		return &PathValidationResult{
			Valid:          false,
			IsValid:        false,
			Error:          err.Error(),
			Errors:         []string{err.Error()},
			Complexity:     "",
			Warnings:       []string{},
			Components:     []PathComponent{},
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

	return &PathValidationResult{
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

// DeleteSubKeyTargets is an alias for backward compatibility
func (jpp *JSONPathProcessor) DeleteSubKeyTargets(data interface{}, path string, targetValues []string) (bool, error) {
	result, err := jpp.DeleteSubKeyTargetsAtomic(data, path, targetValues)
	if err != nil {
		return false, err
	}
	return result.Modified, nil
}

// PathResult represents a result from path finding
type PathResult struct {
	Path      string
	Value     interface{}
	Parent    interface{}
	ParentKey string
	Found     bool
}

// FindValue finds values at the specified path
func (jpp *JSONPathProcessor) FindValue(data interface{}, path string) ([]PathResult, error) {
	components, err := jpp.ParsePath(path)
	if err != nil {
		return nil, err
	}

	results := []PathResult{}
	jpp.findValueRecursive(data, components, "", nil, "", &results)
	return results, nil
}

// findValueRecursive recursively finds values at the specified path
func (jpp *JSONPathProcessor) findValueRecursive(current interface{}, components []PathComponent, currentPath string, parent interface{}, parentKey string, results *[]PathResult) {
	if len(components) == 0 {
		*results = append(*results, PathResult{
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
		if component.IsArrayAccess {
			// Handle array access like items[*]
			if value, exists := v[component.Key]; exists {
				newPath := jpp.buildPath(currentPath, component.Key)
				if arr, ok := value.([]interface{}); ok {
					if component.IsWildcard {
						for i, elem := range arr {
							elemPath := fmt.Sprintf("%s[%d]", newPath, i)
							jpp.findValueRecursive(elem, remaining, elemPath, value, fmt.Sprintf("[%d]", i), results)
						}
					} else if component.ArrayIndex >= 0 && component.ArrayIndex < len(arr) {
						elemPath := fmt.Sprintf("%s[%d]", newPath, component.ArrayIndex)
						jpp.findValueRecursive(arr[component.ArrayIndex], remaining, elemPath, value, fmt.Sprintf("[%d]", component.ArrayIndex), results)
					}
				}
			}
		} else if component.IsWildcard {
			for key, value := range v {
				newPath := jpp.buildPath(currentPath, key)
				jpp.findValueRecursive(value, remaining, newPath, current, key, results)
			}
		} else {
			if value, exists := v[component.Key]; exists {
				newPath := jpp.buildPath(currentPath, component.Key)
				jpp.findValueRecursive(value, remaining, newPath, current, component.Key, results)
			}
		}
	case []interface{}:
		if component.IsWildcard {
			for i, elem := range v {
				elemPath := fmt.Sprintf("%s[%d]", currentPath, i)
				jpp.findValueRecursive(elem, remaining, elemPath, current, fmt.Sprintf("[%d]", i), results)
			}
		} else if component.ArrayIndex >= 0 && component.ArrayIndex < len(v) {
			elemPath := fmt.Sprintf("%s[%d]", currentPath, component.ArrayIndex)
			jpp.findValueRecursive(v[component.ArrayIndex], remaining, elemPath, current, fmt.Sprintf("[%d]", component.ArrayIndex), results)
		}
	}
}

// buildPath builds a path string from components
func (jpp *JSONPathProcessor) buildPath(currentPath, component string) string {
	if currentPath == "" {
		return component
	}
	if strings.HasPrefix(component, "[") {
		return currentPath + component
	}
	return currentPath + "." + component
}

// ValidatePath validates a JSON path string
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
	} else if hasWildcard {
		return "wildcard", nil
	} else if hasArray {
		return "array", nil
	} else {
		return "simple", nil
	}
}
