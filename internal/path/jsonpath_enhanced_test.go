// internal/path/jsonpath_enhanced_test.go
package path

import (
	"testing"
)

func TestDeleteSubKeyTargetsAtomic(t *testing.T) {
	processor := NewJSONPathProcessor()

	tests := []struct {
		name         string
		data         map[string]interface{}
		path         string
		targetValues []string
		expectModify bool
		expectError  bool
		validateFunc func(t *testing.T, data map[string]interface{}, result *AtomicDeletionResult)
	}{
		{
			name: "Delete simple nested array elements",
			data: map[string]interface{}{
				"id": 1,
				"items": []interface{}{
					map[string]interface{}{"subId": "a"},
					map[string]interface{}{"subId": "b"},
					map[string]interface{}{"subId": "c"},
				},
			},
			path:         "items[*].subId",
			targetValues: []string{"a", "c"},
			expectModify: true,
			expectError:  false,
			validateFunc: func(t *testing.T, data map[string]interface{}, result *AtomicDeletionResult) {
				items := data["items"].([]interface{})
				if len(items) != 1 {
					t.Errorf("Expected 1 item remaining, got %d", len(items))
				}

				remainingItem := items[0].(map[string]interface{})
				if remainingItem["subId"] != "b" {
					t.Errorf("Expected remaining item to have subId 'b', got %v", remainingItem["subId"])
				}

				// Test rollback
				if result.Rollback != nil {
					if err := result.Rollback(); err != nil {
						t.Errorf("Rollback failed: %v", err)
					}

					// Verify rollback restored original state
					restoredItems := data["items"].([]interface{})
					if len(restoredItems) != 3 {
						t.Errorf("After rollback, expected 3 items, got %d", len(restoredItems))
					}
				}
			},
		},
		{
			name: "Delete specific array index",
			data: map[string]interface{}{
				"users": []interface{}{
					map[string]interface{}{"id": 1, "name": "Alice"},
					map[string]interface{}{"id": 2, "name": "Bob"},
					map[string]interface{}{"id": 3, "name": "Charlie"},
				},
			},
			path:         "users[1].name",
			targetValues: []string{"Bob"},
			expectModify: true,
			expectError:  false,
			validateFunc: func(t *testing.T, data map[string]interface{}, result *AtomicDeletionResult) {
				users := data["users"].([]interface{})
				bobUser := users[1].(map[string]interface{})

				if _, exists := bobUser["name"]; exists {
					t.Errorf("Expected 'name' to be deleted from Bob's record")
				}

				if bobUser["id"] != 2 {
					t.Errorf("Expected Bob's ID to remain, got %v", bobUser["id"])
				}
			},
		},
		{
			name: "Delete with wildcard matching",
			data: map[string]interface{}{
				"products": []interface{}{
					map[string]interface{}{"category": "electronics", "price": 100},
					map[string]interface{}{"category": "books", "price": 20},
					map[string]interface{}{"category": "electronics", "price": 200},
				},
			},
			path:         "products[*].category",
			targetValues: []string{"electronics"},
			expectModify: true,
			expectError:  false,
			validateFunc: func(t *testing.T, data map[string]interface{}, result *AtomicDeletionResult) {
				products := data["products"].([]interface{})

				electronicsCount := 0
				for _, product := range products {
					productMap := product.(map[string]interface{})
					if category, exists := productMap["category"]; exists && category == "electronics" {
						electronicsCount++
					}
				}

				if electronicsCount != 0 {
					t.Errorf("Expected no electronics categories to remain, found %d", electronicsCount)
				}

				// Check that books category and prices remain
				for _, product := range products {
					productMap := product.(map[string]interface{})
					if category, exists := productMap["category"]; exists && category == "books" {
						if price, exists := productMap["price"]; !exists || price != 20 {
							t.Errorf("Expected books category to remain with price 20, got %v", price)
						}
					}
				}
			},
		},
		{
			name: "Complex nested structure",
			data: map[string]interface{}{
				"orders": []interface{}{
					map[string]interface{}{
						"id": 1,
						"items": []interface{}{
							map[string]interface{}{"productId": "p1", "quantity": 2},
							map[string]interface{}{"productId": "p2", "quantity": 1},
						},
					},
					map[string]interface{}{
						"id": 2,
						"items": []interface{}{
							map[string]interface{}{"productId": "p1", "quantity": 5},
							map[string]interface{}{"productId": "p3", "quantity": 3},
						},
					},
				},
			},
			path:         "orders[*].items[*].productId",
			targetValues: []string{"p1"},
			expectModify: true,
			expectError:  false,
			validateFunc: func(t *testing.T, data map[string]interface{}, result *AtomicDeletionResult) {
				orders := data["orders"].([]interface{})

				// Check that p1 productId was removed from all items
				for _, order := range orders {
					orderMap := order.(map[string]interface{})
					items := orderMap["items"].([]interface{})

					for _, item := range items {
						itemMap := item.(map[string]interface{})
						if productId, exists := itemMap["productId"]; exists && productId == "p1" {
							t.Errorf("Expected productId 'p1' to be deleted, but found it in order %v", orderMap["id"])
						}
					}
				}
			},
		},
		{
			name: "No matches - should not modify",
			data: map[string]interface{}{
				"data": []interface{}{
					map[string]interface{}{"key": "value1"},
					map[string]interface{}{"key": "value2"},
				},
			},
			path:         "data[*].key",
			targetValues: []string{"nonexistent"},
			expectModify: false,
			expectError:  false,
			validateFunc: func(t *testing.T, data map[string]interface{}, result *AtomicDeletionResult) {
				// Data should remain unchanged
				dataArray := data["data"].([]interface{})
				if len(dataArray) != 2 {
					t.Errorf("Expected data to remain unchanged, got %d items", len(dataArray))
				}
			},
		},
		{
			name: "Invalid path - should error",
			data: map[string]interface{}{
				"test": "value",
			},
			path:         "invalid[path",
			targetValues: []string{"value"},
			expectModify: false,
			expectError:  true,
			validateFunc: func(t *testing.T, data map[string]interface{}, result *AtomicDeletionResult) {
				if result.Success {
					t.Errorf("Expected operation to fail with invalid path")
				}
			},
		},
		{
			name: "Array index out of bounds - should not error",
			data: map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{"id": 1},
				},
			},
			path:         "items[5].id",
			targetValues: []string{"1"},
			expectModify: false,
			expectError:  false,
			validateFunc: func(t *testing.T, data map[string]interface{}, result *AtomicDeletionResult) {
				// Should not modify anything since index is out of bounds
				items := data["items"].([]interface{})
				if len(items) != 1 {
					t.Errorf("Expected items to remain unchanged, got %d items", len(items))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a deep copy of the test data
			originalData := deepCopy(tt.data)

			result, err := processor.DeleteSubKeyTargetsAtomic(tt.data, tt.path, tt.targetValues)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result.Modified != tt.expectModify {
				t.Errorf("Expected modified=%v, got %v", tt.expectModify, result.Modified)
			}

			if result.Success != !tt.expectError {
				t.Errorf("Expected success=%v, got %v", !tt.expectError, result.Success)
			}

			// Run custom validation
			if tt.validateFunc != nil {
				tt.validateFunc(t, tt.data, result)
			}

			// Test that we can restore original data if rollback is available
			if result.Rollback != nil && tt.expectModify {
				if err := result.Rollback(); err != nil {
					t.Errorf("Rollback failed: %v", err)
				}

				// Verify data was restored (basic check)
				if !deepEqual(tt.data, originalData) {
					t.Errorf("Data was not properly restored after rollback")
				}
			}
		})
	}
}

func TestEnhancedPathParsing(t *testing.T) {
	processor := NewJSONPathProcessor()

	tests := []struct {
		name        string
		path        string
		expectError bool
		expected    []PathComponent
	}{
		{
			name:        "Simple path",
			path:        "user.name",
			expectError: false,
			expected: []PathComponent{
				{Key: "user", IsArrayAccess: false, IsWildcard: false, ArrayIndex: -1},
				{Key: "name", IsArrayAccess: false, IsWildcard: false, ArrayIndex: -1},
			},
		},
		{
			name:        "Array access",
			path:        "users[0].name",
			expectError: false,
			expected: []PathComponent{
				{Key: "users", IsArrayAccess: true, IsWildcard: false, ArrayIndex: 0},
				{Key: "name", IsArrayAccess: false, IsWildcard: false, ArrayIndex: -1},
			},
		},
		{
			name:        "Wildcard array access",
			path:        "users[*].name",
			expectError: false,
			expected: []PathComponent{
				{Key: "users", IsArrayAccess: true, IsWildcard: true, ArrayIndex: -1},
				{Key: "name", IsArrayAccess: false, IsWildcard: false, ArrayIndex: -1},
			},
		},
		{
			name:        "Complex nested path",
			path:        "orders[*].items[0].product.name",
			expectError: false,
			expected: []PathComponent{
				{Key: "orders", IsArrayAccess: true, IsWildcard: true, ArrayIndex: -1},
				{Key: "items", IsArrayAccess: true, IsWildcard: false, ArrayIndex: 0},
				{Key: "product", IsArrayAccess: false, IsWildcard: false, ArrayIndex: -1},
				{Key: "name", IsArrayAccess: false, IsWildcard: false, ArrayIndex: -1},
			},
		},
		{
			name:        "Invalid bracket syntax",
			path:        "users[invalid].name",
			expectError: true,
			expected:    nil,
		},
		{
			name:        "Unmatched brackets",
			path:        "users[0.name",
			expectError: true,
			expected:    nil,
		},
		{
			name:        "Negative array index",
			path:        "users[-1].name",
			expectError: true,
			expected:    nil,
		},
		{
			name:        "Empty path",
			path:        "",
			expectError: true,
			expected:    nil,
		},
		{
			name:        "Path with spaces",
			path:        "user name.value",
			expectError: true,
			expected:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			components, err := processor.ParsePath(tt.path)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if len(components) != len(tt.expected) {
				t.Errorf("Expected %d components, got %d", len(tt.expected), len(components))
				return
			}

			for i, component := range components {
				expected := tt.expected[i]
				if component.Key != expected.Key {
					t.Errorf("Component %d: expected key %s, got %s", i, expected.Key, component.Key)
				}
				if component.IsArrayAccess != expected.IsArrayAccess {
					t.Errorf("Component %d: expected IsArrayAccess %v, got %v", i, expected.IsArrayAccess, component.IsArrayAccess)
				}
				if component.IsWildcard != expected.IsWildcard {
					t.Errorf("Component %d: expected IsWildcard %v, got %v", i, expected.IsWildcard, component.IsWildcard)
				}
				if component.ArrayIndex != expected.ArrayIndex {
					t.Errorf("Component %d: expected ArrayIndex %d, got %d", i, expected.ArrayIndex, component.ArrayIndex)
				}
			}
		})
	}
}

func TestPathValidation(t *testing.T) {
	processor := NewJSONPathProcessor()

	tests := []struct {
		name                 string
		path                 string
		expectValid          bool
		expectComplexity     string
		expectWarnings       int
		expectComponentCount int
	}{
		{
			name:                 "Simple valid path",
			path:                 "user.name",
			expectValid:          true,
			expectComplexity:     "simple",
			expectWarnings:       0,
			expectComponentCount: 2,
		},
		{
			name:                 "Complex path with arrays",
			path:                 "orders[*].items[0].product",
			expectValid:          true,
			expectComplexity:     "complex",
			expectWarnings:       0,
			expectComponentCount: 3,
		},
		{
			name:                 "Very complex path",
			path:                 "data[*].nested[*].deep[0].value",
			expectValid:          true,
			expectComplexity:     "very_complex",
			expectWarnings:       1, // Warning for very complex path
			expectComponentCount: 4,
		},
		{
			name:                 "Invalid syntax",
			path:                 "invalid[path",
			expectValid:          false,
			expectComplexity:     "",
			expectWarnings:       0,
			expectComponentCount: 0,
		},
		{
			name:                 "Very deep path",
			path:                 "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p",
			expectValid:          true,
			expectComplexity:     "complex",
			expectWarnings:       1, // Warning for deep path
			expectComponentCount: 16,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := processor.ValidatePathDetailed(tt.path)

			if result.IsValid != tt.expectValid {
				t.Errorf("Expected valid=%v, got %v. Errors: %v", tt.expectValid, result.IsValid, result.Errors)
			}

			if result.IsValid && result.Complexity != tt.expectComplexity {
				t.Errorf("Expected complexity %s, got %s", tt.expectComplexity, result.Complexity)
			}

			if len(result.Warnings) != tt.expectWarnings {
				t.Errorf("Expected %d warnings, got %d: %v", tt.expectWarnings, len(result.Warnings), result.Warnings)
			}

			if result.ComponentCount != tt.expectComponentCount {
				t.Errorf("Expected %d components, got %d", tt.expectComponentCount, result.ComponentCount)
			}
		})
	}
}

func TestTransactionRollback(t *testing.T) {
	processor := NewJSONPathProcessor()

	data := map[string]interface{}{
		"users": []interface{}{
			map[string]interface{}{"id": 1, "name": "Alice", "email": "alice@example.com"},
			map[string]interface{}{"id": 2, "name": "Bob", "email": "bob@example.com"},
		},
	}

	// Store original state
	originalData := deepCopy(data)

	// Perform atomic deletion
	result, err := processor.DeleteSubKeyTargetsAtomic(data, "users[*].email", []string{"alice@example.com", "bob@example.com"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !result.Modified {
		t.Errorf("Expected data to be modified")
	}

	// Verify emails were deleted
	users := data["users"].([]interface{})
	for _, user := range users {
		userMap := user.(map[string]interface{})
		if _, exists := userMap["email"]; exists {
			t.Errorf("Expected email to be deleted from user %v", userMap["id"])
		}
	}

	// Test rollback
	if result.Rollback == nil {
		t.Fatalf("Expected rollback function to be available")
	}

	err = result.Rollback()
	if err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}

	// Verify data was restored
	if !deepEqual(data, originalData) {
		t.Errorf("Data was not properly restored after rollback")
	}

	// Verify emails were restored
	users = data["users"].([]interface{})
	for i, user := range users {
		userMap := user.(map[string]interface{})
		if email, exists := userMap["email"]; !exists {
			t.Errorf("Expected email to be restored for user %d", i)
		} else {
			expectedEmails := []string{"alice@example.com", "bob@example.com"}
			if email != expectedEmails[i] {
				t.Errorf("Expected email %s, got %s for user %d", expectedEmails[i], email, i)
			}
		}
	}
}

// Helper functions for testing

func deepCopy(data map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for key, value := range data {
		result[key] = deepCopyValue(value)
	}
	return result
}

func deepCopyValue(value interface{}) interface{} {
	switch v := value.(type) {
	case map[string]interface{}:
		result := make(map[string]interface{})
		for k, val := range v {
			result[k] = deepCopyValue(val)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, val := range v {
			result[i] = deepCopyValue(val)
		}
		return result
	default:
		return value
	}
}

func deepEqual(a, b map[string]interface{}) bool {
	if len(a) != len(b) {
		return false
	}

	for key, valueA := range a {
		valueB, exists := b[key]
		if !exists {
			return false
		}

		if !deepEqualValue(valueA, valueB) {
			return false
		}
	}

	return true
}

func deepEqualValue(a, b interface{}) bool {
	switch va := a.(type) {
	case map[string]interface{}:
		vb, ok := b.(map[string]interface{})
		if !ok {
			return false
		}
		return deepEqual(va, vb)
	case []interface{}:
		vb, ok := b.([]interface{})
		if !ok || len(va) != len(vb) {
			return false
		}
		for i, itemA := range va {
			if !deepEqualValue(itemA, vb[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}
