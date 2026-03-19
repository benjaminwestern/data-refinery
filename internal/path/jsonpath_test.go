// internal/path/jsonpath_test.go
package path

import (
	"testing"
)

func TestJSONPathProcessor_ParsePath(t *testing.T) {
	jpp := NewJSONPathProcessor()

	tests := []struct {
		name     string
		path     string
		expected int
		hasError bool
	}{
		{
			name:     "simple path",
			path:     "id",
			expected: 1,
			hasError: false,
		},
		{
			name:     "nested path",
			path:     "user.id",
			expected: 2,
			hasError: false,
		},
		{
			name:     "array access",
			path:     "items[0].id",
			expected: 2,
			hasError: false,
		},
		{
			name:     "wildcard array",
			path:     "items[*].id",
			expected: 2,
			hasError: false,
		},
		{
			name:     "empty path",
			path:     "",
			expected: 0,
			hasError: true,
		},
		{
			name:     "path with leading dot",
			path:     ".user.id",
			expected: 2,
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			components, err := jpp.ParsePath(tt.path)

			if tt.hasError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(components) != tt.expected {
				t.Errorf("Expected %d components, got %d", tt.expected, len(components))
			}
		})
	}
}

func TestJSONPathProcessor_DeleteSubKeyTargets(t *testing.T) {
	jpp := NewJSONPathProcessor()

	tests := []struct {
		name         string
		data         map[string]interface{}
		path         string
		targetValues []string
		expected     bool
		resultCheck  func(map[string]interface{}) bool
	}{
		{
			name: "delete from array",
			data: map[string]interface{}{
				"id": 1,
				"items": []interface{}{
					map[string]interface{}{"subId": "a", "value": 1},
					map[string]interface{}{"subId": "b", "value": 2},
					map[string]interface{}{"subId": "c", "value": 3},
				},
			},
			path:         "items[*].subId",
			targetValues: []string{"a"},
			expected:     true,
			resultCheck: func(data map[string]interface{}) bool {
				items := data["items"].([]interface{})
				if len(items) != 3 {
					return false
				}
				// Check that the item with subId "a" no longer has subId
				firstItem := items[0].(map[string]interface{})
				_, hasSubId := firstItem["subId"]
				return !hasSubId && firstItem["value"] == 1
			},
		},
		{
			name: "delete specific array element",
			data: map[string]interface{}{
				"id": 1,
				"items": []interface{}{
					map[string]interface{}{"subId": "a", "value": 1},
					map[string]interface{}{"subId": "b", "value": 2},
					map[string]interface{}{"subId": "c", "value": 3},
				},
			},
			path:         "items[0].subId",
			targetValues: []string{"a"},
			expected:     true,
			resultCheck: func(data map[string]interface{}) bool {
				items := data["items"].([]interface{})
				if len(items) != 3 {
					return false
				}
				// Check that the first item no longer has subId
				firstItem := items[0].(map[string]interface{})
				_, hasSubId := firstItem["subId"]
				return !hasSubId && firstItem["value"] == 1
			},
		},
		{
			name: "delete nested object key",
			data: map[string]interface{}{
				"id": 1,
				"user": map[string]interface{}{
					"name":  "John",
					"email": "john@example.com",
				},
			},
			path:         "user.email",
			targetValues: []string{"john@example.com"},
			expected:     true,
			resultCheck: func(data map[string]interface{}) bool {
				user := data["user"].(map[string]interface{})
				_, hasEmail := user["email"]
				return !hasEmail && user["name"] == "John"
			},
		},
		{
			name: "no match - no deletion",
			data: map[string]interface{}{
				"id": 1,
				"items": []interface{}{
					map[string]interface{}{"subId": "a", "value": 1},
					map[string]interface{}{"subId": "b", "value": 2},
				},
			},
			path:         "items[*].subId",
			targetValues: []string{"z"},
			expected:     false,
			resultCheck: func(data map[string]interface{}) bool {
				items := data["items"].([]interface{})
				if len(items) != 2 {
					return false
				}
				// Check that both items still have subId
				firstItem := items[0].(map[string]interface{})
				secondItem := items[1].(map[string]interface{})
				return firstItem["subId"] == "a" && secondItem["subId"] == "b"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			modified, err := jpp.DeleteSubKeyTargets(tt.data, tt.path, tt.targetValues)

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if modified != tt.expected {
				t.Errorf("Expected modified=%v, got %v", tt.expected, modified)
			}

			if tt.resultCheck != nil && !tt.resultCheck(tt.data) {
				t.Error("Result check failed")
			}
		})
	}
}

func TestJSONPathProcessor_FindValue(t *testing.T) {
	jpp := NewJSONPathProcessor()

	data := map[string]interface{}{
		"id": 1,
		"items": []interface{}{
			map[string]interface{}{"subId": "a", "value": 1},
			map[string]interface{}{"subId": "b", "value": 2},
			map[string]interface{}{"subId": "c", "value": 3},
		},
		"user": map[string]interface{}{
			"name":  "John",
			"email": "john@example.com",
		},
	}

	tests := []struct {
		name          string
		path          string
		expectedCount int
		hasError      bool
	}{
		{
			name:          "find simple value",
			path:          "id",
			expectedCount: 1,
			hasError:      false,
		},
		{
			name:          "find nested value",
			path:          "user.name",
			expectedCount: 1,
			hasError:      false,
		},
		{
			name:          "find array elements",
			path:          "items[*].subId",
			expectedCount: 3,
			hasError:      false,
		},
		{
			name:          "find specific array element",
			path:          "items[0].subId",
			expectedCount: 1,
			hasError:      false,
		},
		{
			name:          "find non-existent path",
			path:          "nonexistent",
			expectedCount: 0,
			hasError:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := jpp.FindValue(data, tt.path)

			if tt.hasError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(results) != tt.expectedCount {
				t.Errorf("Expected %d results, got %d", tt.expectedCount, len(results))
			}
		})
	}
}

func TestJSONPathProcessor_ValidatePath(t *testing.T) {
	jpp := NewJSONPathProcessor()

	tests := []struct {
		name     string
		path     string
		hasError bool
	}{
		{
			name:     "valid simple path",
			path:     "id",
			hasError: false,
		},
		{
			name:     "valid nested path",
			path:     "user.name",
			hasError: false,
		},
		{
			name:     "valid array path",
			path:     "items[0].id",
			hasError: false,
		},
		{
			name:     "valid wildcard array path",
			path:     "items[*].id",
			hasError: false,
		},
		{
			name:     "invalid empty path",
			path:     "",
			hasError: true,
		},
		{
			name:     "invalid array syntax",
			path:     "items[invalid].id",
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := jpp.ValidatePath(tt.path)

			if tt.hasError {
				if err == nil {
					t.Error("Expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

func TestJSONPathProcessor_GetPathType(t *testing.T) {
	jpp := NewJSONPathProcessor()

	tests := []struct {
		name     string
		path     string
		expected string
		hasError bool
	}{
		{
			name:     "simple path",
			path:     "id",
			expected: "simple",
			hasError: false,
		},
		{
			name:     "array path",
			path:     "items[0].id",
			expected: "array",
			hasError: false,
		},
		{
			name:     "wildcard path",
			path:     "*.id",
			expected: "wildcard",
			hasError: false,
		},
		{
			name:     "wildcard array path",
			path:     "items[*].id",
			expected: "wildcard_array",
			hasError: false,
		},
		{
			name:     "invalid path",
			path:     "",
			expected: "",
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pathType, err := jpp.GetPathType(tt.path)

			if tt.hasError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if pathType != tt.expected {
				t.Errorf("Expected path type %s, got %s", tt.expected, pathType)
			}
		})
	}
}
