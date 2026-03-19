package components

import (
	"fmt"
	"strings"
	"testing"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbletea"
)

func TestInputComponentCreation(t *testing.T) {
	ic := NewInputComponent(
		WithTitle("Test Input"),
		WithPlaceholder("Enter test value"),
		WithDescription("This is a test input"),
		WithWidth(50),
		WithSubmitHandler(func(value string) tea.Cmd {
			return nil
		}),
	)

	if ic == nil {
		t.Fatal("Expected input component to be non-nil")
	}

	if ic.title != "Test Input" {
		t.Errorf("Expected title to be 'Test Input', got %s", ic.title)
	}

	if ic.placeholder != "Enter test value" {
		t.Errorf("Expected placeholder to be 'Enter test value', got %s", ic.placeholder)
	}

	if ic.description != "This is a test input" {
		t.Errorf("Expected description to be 'This is a test input', got %s", ic.description)
	}

	if ic.width != 50 {
		t.Errorf("Expected width to be 50, got %d", ic.width)
	}
}

func TestInputComponentSubmit(t *testing.T) {
	var submitted bool
	var submittedValue string

	ic := NewInputComponent(
		WithTitle("Test Input"),
		WithSubmitHandler(func(value string) tea.Cmd {
			submitted = true
			submittedValue = value
			return nil
		}),
	)

	// Set a value
	ic.SetValue("test value")

	// Simulate Enter key press
	enterMsg := tea.KeyMsg{Type: tea.KeyEnter}
	_, cmd := ic.Update(enterMsg)

	if !submitted {
		t.Error("Expected input to be submitted")
	}

	if submittedValue != "test value" {
		t.Errorf("Expected submitted value to be 'test value', got %s", submittedValue)
	}

	if cmd != nil {
		t.Log("Update returned a command")
	}

	if !ic.IsSubmitted() {
		t.Error("Expected input to be marked as submitted")
	}
}

func TestInputComponentValidation(t *testing.T) {
	ic := NewInputComponent(
		WithTitle("Test Input"),
		WithValidation(func(value string) error {
			if value == "" {
				return nil // Allow empty for this test
			}
			if len(value) < 3 {
				return fmt.Errorf("value must be at least 3 characters")
			}
			return nil
		}),
	)

	// Test invalid value
	ic.SetValue("ab")
	enterMsg := tea.KeyMsg{Type: tea.KeyEnter}
	_, _ = ic.Update(enterMsg)

	if ic.errorMessage == "" {
		t.Error("Expected error message for invalid input")
	}

	if ic.IsSubmitted() {
		t.Error("Expected input not to be submitted with invalid value")
	}

	// Test valid value
	ic.SetValue("abc")
	_, _ = ic.Update(enterMsg)

	if ic.errorMessage != "" {
		t.Errorf("Expected no error message for valid input, got: %s", ic.errorMessage)
	}

	if !ic.IsSubmitted() {
		t.Error("Expected input to be submitted with valid value")
	}
}

func TestInputComponentReset(t *testing.T) {
	ic := NewInputComponent(
		WithTitle("Test Input"),
	)

	// Set value and submit
	ic.SetValue("test value")
	ic.submitted = true
	ic.errorMessage = "test error"

	// Reset
	ic.Reset()

	if ic.GetValue() != "" {
		t.Errorf("Expected empty value after reset, got %s", ic.GetValue())
	}

	if ic.IsSubmitted() {
		t.Error("Expected input not to be submitted after reset")
	}

	if ic.errorMessage != "" {
		t.Errorf("Expected no error message after reset, got %s", ic.errorMessage)
	}
}

func TestInputComponentEscape(t *testing.T) {
	ic := NewInputComponent(
		WithTitle("Test Input"),
	)

	// Set value and error
	ic.SetValue("test value")
	ic.errorMessage = "test error"
	ic.submitted = true

	// Simulate Escape key press
	escMsg := tea.KeyMsg{Type: tea.KeyEsc}
	_, _ = ic.Update(escMsg)

	if ic.GetValue() != "" {
		t.Errorf("Expected empty value after escape, got %s", ic.GetValue())
	}

	if ic.IsSubmitted() {
		t.Error("Expected input not to be submitted after escape")
	}

	if ic.errorMessage != "" {
		t.Errorf("Expected no error message after escape, got %s", ic.errorMessage)
	}
}

func TestInputComponentFocus(t *testing.T) {
	ic := NewInputComponent(
		WithTitle("Test Input"),
	)

	// Initially focused
	if !ic.IsFocused() {
		t.Error("Expected input to be focused initially")
	}

	// Blur
	ic.Blur()
	if ic.IsFocused() {
		t.Error("Expected input to be unfocused after blur")
	}

	// Focus again
	ic.Focus()
	if !ic.IsFocused() {
		t.Error("Expected input to be focused after focus")
	}
}

func TestPathInputComponent(t *testing.T) {
	var submitted bool
	var submittedValue string

	pic := NewPathInputComponent(func(value string) tea.Cmd {
		submitted = true
		submittedValue = value
		return nil
	})

	if pic.title != "Data Source Path" {
		t.Errorf("Expected title to be 'Data Source Path', got %s", pic.title)
	}

	// Test empty path validation
	pic.SetValue("")
	enterMsg := tea.KeyMsg{Type: tea.KeyEnter}
	_, _ = pic.Update(enterMsg)

	if pic.errorMessage == "" {
		t.Error("Expected error message for empty path")
	}

	if submitted {
		t.Error("Expected path not to be submitted when empty")
	}

	// Test valid path
	pic.SetValue("/valid/path")
	_, _ = pic.Update(enterMsg)

	if pic.errorMessage != "" {
		t.Errorf("Expected no error message for valid path, got: %s", pic.errorMessage)
	}

	if !submitted {
		t.Error("Expected path to be submitted for valid value")
	}

	if submittedValue != "/valid/path" {
		t.Errorf("Expected submitted value to be '/valid/path', got %s", submittedValue)
	}
}

func TestKeyInputComponent(t *testing.T) {
	var submitted bool
	var submittedValue string

	kic := NewKeyInputComponent(func(value string) tea.Cmd {
		submitted = true
		submittedValue = value
		return nil
	})

	if kic.title != "GCS Service Account Key" {
		t.Errorf("Expected title to be 'GCS Service Account Key', got %s", kic.title)
	}

	// Test empty key validation
	kic.SetValue("")
	enterMsg := tea.KeyMsg{Type: tea.KeyEnter}
	_, _ = kic.Update(enterMsg)

	if kic.errorMessage == "" {
		t.Error("Expected error message for empty key")
	}

	if submitted {
		t.Error("Expected key not to be submitted when empty")
	}

	// Test invalid key format
	kic.SetValue("/path/to/key.txt")
	_, _ = kic.Update(enterMsg)

	if kic.errorMessage == "" {
		t.Error("Expected error message for non-JSON key file")
	}

	// Test valid key
	kic.SetValue("/path/to/key.json")
	_, _ = kic.Update(enterMsg)

	if kic.errorMessage != "" {
		t.Errorf("Expected no error message for valid key, got: %s", kic.errorMessage)
	}

	if !submitted {
		t.Error("Expected key to be submitted for valid value")
	}

	if submittedValue != "/path/to/key.json" {
		t.Errorf("Expected submitted value to be '/path/to/key.json', got %s", submittedValue)
	}
}

func TestUniqueKeyInputComponent(t *testing.T) {
	var submitted bool
	var submittedValue string

	ukic := NewUniqueKeyInputComponent(func(value string) tea.Cmd {
		submitted = true
		submittedValue = value
		return nil
	})

	if ukic.title != "Unique Key Field" {
		t.Errorf("Expected title to be 'Unique Key Field', got %s", ukic.title)
	}

	// Test empty key validation
	ukic.SetValue("")
	enterMsg := tea.KeyMsg{Type: tea.KeyEnter}
	_, _ = ukic.Update(enterMsg)

	if ukic.errorMessage == "" {
		t.Error("Expected error message for empty unique key")
	}

	if submitted {
		t.Error("Expected unique key not to be submitted when empty")
	}

	// Test invalid key with whitespace
	ukic.SetValue("user id")
	_, _ = ukic.Update(enterMsg)

	if ukic.errorMessage == "" {
		t.Error("Expected error message for unique key with whitespace")
	}

	// Test valid key
	ukic.SetValue("user_id")
	_, _ = ukic.Update(enterMsg)

	if ukic.errorMessage != "" {
		t.Errorf("Expected no error message for valid unique key, got: %s", ukic.errorMessage)
	}

	if !submitted {
		t.Error("Expected unique key to be submitted for valid value")
	}

	if submittedValue != "user_id" {
		t.Errorf("Expected submitted value to be 'user_id', got %s", submittedValue)
	}
}

func TestWorkersInputComponent(t *testing.T) {
	var submitted bool
	var submittedValue string

	wic := NewWorkersInputComponent(func(value string) tea.Cmd {
		submitted = true
		submittedValue = value
		return nil
	})

	if wic.title != "Number of Workers" {
		t.Errorf("Expected title to be 'Number of Workers', got %s", wic.title)
	}

	// Test empty workers validation
	wic.SetValue("")
	enterMsg := tea.KeyMsg{Type: tea.KeyEnter}
	_, _ = wic.Update(enterMsg)

	if wic.errorMessage == "" {
		t.Error("Expected error message for empty workers")
	}

	if submitted {
		t.Error("Expected workers not to be submitted when empty")
	}

	// Test invalid number
	wic.SetValue("abc")
	_, _ = wic.Update(enterMsg)

	if wic.errorMessage == "" {
		t.Error("Expected error message for non-numeric workers")
	}

	// Test number out of range
	wic.SetValue("20")
	_, _ = wic.Update(enterMsg)

	if wic.errorMessage == "" {
		t.Error("Expected error message for workers out of range")
	}

	// Test valid workers
	wic.SetValue("4")
	_, _ = wic.Update(enterMsg)

	if wic.errorMessage != "" {
		t.Errorf("Expected no error message for valid workers, got: %s", wic.errorMessage)
	}

	if !submitted {
		t.Error("Expected workers to be submitted for valid value")
	}

	if submittedValue != "4" {
		t.Errorf("Expected submitted value to be '4', got %s", submittedValue)
	}
}

func TestInputComponentSetSize(t *testing.T) {
	ic := NewInputComponent(
		WithTitle("Test Input"),
		WithWidth(50),
	)

	// Test initial size
	if ic.width != 50 {
		t.Errorf("Expected initial width to be 50, got %d", ic.width)
	}

	// Set new size
	ic.SetSize(80, 20)

	if ic.width != 80 {
		t.Errorf("Expected width to be 80 after SetSize, got %d", ic.width)
	}

	if ic.height != 20 {
		t.Errorf("Expected height to be 20 after SetSize, got %d", ic.height)
	}

	// Input width should be adjusted for borders
	expectedInputWidth := 80 - 4
	if ic.input.Width != expectedInputWidth {
		t.Errorf("Expected input width to be %d after SetSize, got %d", expectedInputWidth, ic.input.Width)
	}
}

func TestInputComponentView(t *testing.T) {
	ic := NewInputComponent(
		WithTitle("Test Input"),
		WithDescription("Test description"),
		WithPlaceholder("Enter value"),
	)

	view := ic.View()

	if view == "" {
		t.Error("Expected view to be non-empty")
	}

	// View should contain title
	if !strings.Contains(view, "Test Input") {
		t.Error("Expected view to contain title")
	}

	// View should contain description
	if !strings.Contains(view, "Test description") {
		t.Error("Expected view to contain description")
	}

	// View should contain help text
	if !strings.Contains(view, "Enter: Submit") {
		t.Error("Expected view to contain help text")
	}
}

func TestInputComponentWithEchoMode(t *testing.T) {
	ic := NewInputComponent(
		WithTitle("Password Input"),
		WithEchoMode(textinput.EchoPassword),
	)

	if ic.input.EchoMode != textinput.EchoPassword {
		t.Error("Expected echo mode to be EchoPassword")
	}
}

func TestInputComponentWithCharLimit(t *testing.T) {
	ic := NewInputComponent(
		WithTitle("Limited Input"),
		WithCharLimit(10),
	)

	if ic.input.CharLimit != 10 {
		t.Errorf("Expected character limit to be 10, got %d", ic.input.CharLimit)
	}
}
