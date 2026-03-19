package components

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// InputComponent represents a general input component for various input types
type InputComponent struct {
	BaseComponent
	input        textinput.Model
	title        string
	placeholder  string
	description  string
	validateFunc func(string) error
	submitFunc   func(string) tea.Cmd
	errorMessage string
	submitted    bool
	width        int
	height       int
}

// InputComponentOption is a function option for configuring InputComponent
type InputComponentOption func(*InputComponent)

// WithTitle sets the title for the input component
func WithTitle(title string) InputComponentOption {
	return func(ic *InputComponent) {
		ic.title = title
	}
}

// WithPlaceholder sets the placeholder text
func WithPlaceholder(placeholder string) InputComponentOption {
	return func(ic *InputComponent) {
		ic.placeholder = placeholder
		ic.input.Placeholder = placeholder
	}
}

// WithDescription sets the description text
func WithDescription(description string) InputComponentOption {
	return func(ic *InputComponent) {
		ic.description = description
	}
}

// WithValidation sets the validation function
func WithValidation(fn func(string) error) InputComponentOption {
	return func(ic *InputComponent) {
		ic.validateFunc = fn
	}
}

// WithSubmitHandler sets the submit handler
func WithSubmitHandler(fn func(string) tea.Cmd) InputComponentOption {
	return func(ic *InputComponent) {
		ic.submitFunc = fn
	}
}

// WithWidth sets the input width
func WithWidth(width int) InputComponentOption {
	return func(ic *InputComponent) {
		ic.width = width
		ic.input.Width = width
	}
}

// WithCharLimit sets the character limit
func WithCharLimit(limit int) InputComponentOption {
	return func(ic *InputComponent) {
		ic.input.CharLimit = limit
	}
}

// WithEchoMode sets the echo mode (for password fields)
func WithEchoMode(mode textinput.EchoMode) InputComponentOption {
	return func(ic *InputComponent) {
		ic.input.EchoMode = mode
	}
}

// NewInputComponent creates a new input component
func NewInputComponent(opts ...InputComponentOption) *InputComponent {
	input := textinput.New()
	input.Focus()

	ic := &InputComponent{
		BaseComponent: BaseComponent{focused: true}, // Initialize as focused
		input:         input,
		title:         "Input",
		placeholder:   "Enter value...",
		width:         50,
		height:        10,
	}

	// Apply options
	for _, opt := range opts {
		opt(ic)
	}

	// Set default width if not specified
	if ic.input.Width == 0 {
		ic.input.Width = ic.width
	}

	return ic
}

// Init initializes the input component
func (ic *InputComponent) Init() tea.Cmd {
	return textinput.Blink
}

// Update handles input component updates
func (ic *InputComponent) Update(msg tea.Msg) (Component, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyEnter:
			if ic.submitted {
				return ic, nil
			}

			value := ic.input.Value()

			// Validate input
			if ic.validateFunc != nil {
				if err := ic.validateFunc(value); err != nil {
					ic.errorMessage = err.Error()
					return ic, nil
				}
			}

			// Clear error message
			ic.errorMessage = ""
			ic.submitted = true

			// Call submit handler
			if ic.submitFunc != nil {
				cmd = ic.submitFunc(value)
			}

			return ic, cmd

		case tea.KeyEsc:
			// Clear input and error
			ic.input.SetValue("")
			ic.errorMessage = ""
			ic.submitted = false
			return ic, nil
		}
	}

	// Update the input model
	ic.input, cmd = ic.input.Update(msg)
	return ic, cmd
}

// View renders the input component
func (ic *InputComponent) View() string {
	var b strings.Builder

	// Title
	titleStyle := lipgloss.NewStyle().
		Bold(true).
		Foreground(primaryColor).
		MarginBottom(1)
	b.WriteString(titleStyle.Render(ic.title))
	b.WriteString("\n")

	// Description
	if ic.description != "" {
		descStyle := lipgloss.NewStyle().
			Foreground(lipgloss.Color("245")).
			MarginBottom(1)
		b.WriteString(descStyle.Render(ic.description))
		b.WriteString("\n")
	}

	// Input field
	inputStyle := lipgloss.NewStyle().
		Border(lipgloss.RoundedBorder()).
		BorderForeground(primaryColor).
		Padding(0, 1)

	if ic.IsFocused() {
		inputStyle = inputStyle.BorderForeground(primaryColor)
	} else {
		inputStyle = inputStyle.BorderForeground(lipgloss.Color("240"))
	}

	b.WriteString(inputStyle.Render(ic.input.View()))
	b.WriteString("\n")

	// Error message
	if ic.errorMessage != "" {
		errorStyle := lipgloss.NewStyle().
			Foreground(errorColor).
			MarginTop(1)
		b.WriteString(errorStyle.Render("Error: " + ic.errorMessage))
		b.WriteString("\n")
	}

	// Help text
	helpStyle := lipgloss.NewStyle().
		Foreground(lipgloss.Color("241")).
		MarginTop(1)

	if ic.submitted {
		b.WriteString(helpStyle.Render("✓ Submitted"))
	} else {
		b.WriteString(helpStyle.Render("Enter: Submit • Esc: Clear"))
	}

	return b.String()
}

// Focus sets focus to the input component
func (ic *InputComponent) Focus() {
	ic.BaseComponent.Focus()
	ic.input.Focus()
}

// Blur removes focus from the input component
func (ic *InputComponent) Blur() {
	ic.BaseComponent.Blur()
	ic.input.Blur()
}

// SetValue sets the input value
func (ic *InputComponent) SetValue(value string) {
	ic.input.SetValue(value)
	ic.submitted = false
	ic.errorMessage = ""
}

// GetValue returns the current input value
func (ic *InputComponent) GetValue() string {
	return ic.input.Value()
}

// IsSubmitted returns whether the input has been submitted
func (ic *InputComponent) IsSubmitted() bool {
	return ic.submitted
}

// Reset resets the input component
func (ic *InputComponent) Reset() {
	ic.input.SetValue("")
	ic.errorMessage = ""
	ic.submitted = false
}

// SetSize sets the component size
func (ic *InputComponent) SetSize(width, height int) {
	ic.width = width
	ic.height = height
	ic.input.Width = width - 4 // Account for borders and padding
}

// PathInputComponent creates a specialized path input component
func NewPathInputComponent(onSubmit func(string) tea.Cmd) *InputComponent {
	return NewInputComponent(
		WithTitle("Data Source Path"),
		WithPlaceholder("Enter path to data source (local file, directory, or GCS bucket)..."),
		WithDescription("Specify the path to your data source. Supports local files, directories, and GCS buckets."),
		WithWidth(70),
		WithCharLimit(500),
		WithValidation(func(value string) error {
			if strings.TrimSpace(value) == "" {
				return fmt.Errorf("path cannot be empty")
			}
			return nil
		}),
		WithSubmitHandler(onSubmit),
	)
}

// KeyInputComponent creates a specialized GCS key input component
func NewKeyInputComponent(onSubmit func(string) tea.Cmd) *InputComponent {
	return NewInputComponent(
		WithTitle("GCS Service Account Key"),
		WithPlaceholder("Enter path to GCS service account key file..."),
		WithDescription("Path to your Google Cloud Service Account JSON key file for GCS access."),
		WithWidth(60),
		WithCharLimit(300),
		WithEchoMode(textinput.EchoNormal), // Don't hide the path
		WithValidation(func(value string) error {
			if strings.TrimSpace(value) == "" {
				return fmt.Errorf("key path cannot be empty")
			}
			if !strings.HasSuffix(strings.ToLower(value), ".json") {
				return fmt.Errorf("key file must be a JSON file")
			}
			return nil
		}),
		WithSubmitHandler(onSubmit),
	)
}

// LogPathInputComponent creates a specialized log path input component
func NewLogPathInputComponent(onSubmit func(string) tea.Cmd) *InputComponent {
	return NewInputComponent(
		WithTitle("Log Output Path"),
		WithPlaceholder("Enter path for log output (optional)..."),
		WithDescription("Specify where to save log files. Leave empty to use default location."),
		WithWidth(60),
		WithCharLimit(300),
		WithValidation(func(value string) error {
			// Log path is optional, so empty is valid
			return nil
		}),
		WithSubmitHandler(onSubmit),
	)
}

// UniqueKeyInputComponent creates a specialized unique key input component
func NewUniqueKeyInputComponent(onSubmit func(string) tea.Cmd) *InputComponent {
	return NewInputComponent(
		WithTitle("Unique Key Field"),
		WithPlaceholder("Enter the field name to use as unique key..."),
		WithDescription("Specify the JSON field name to use for duplicate detection (e.g., 'id', 'email')."),
		WithWidth(50),
		WithCharLimit(100),
		WithValidation(func(value string) error {
			if strings.TrimSpace(value) == "" {
				return fmt.Errorf("unique key field cannot be empty")
			}
			// Basic validation for field names
			if strings.ContainsAny(value, " \t\n\r") {
				return fmt.Errorf("field name cannot contain whitespace")
			}
			return nil
		}),
		WithSubmitHandler(onSubmit),
	)
}

// WorkersInputComponent creates a specialized workers input component
func NewWorkersInputComponent(onSubmit func(string) tea.Cmd) *InputComponent {
	return NewInputComponent(
		WithTitle("Number of Workers"),
		WithPlaceholder("Enter number of concurrent workers (1-16)..."),
		WithDescription("Specify the number of concurrent workers for processing. More workers = faster processing but higher memory usage."),
		WithWidth(30),
		WithCharLimit(3),
		WithValidation(func(value string) error {
			if strings.TrimSpace(value) == "" {
				return fmt.Errorf("number of workers cannot be empty")
			}
			// Parse and validate number
			var workers int
			if _, err := fmt.Sscanf(value, "%d", &workers); err != nil {
				return fmt.Errorf("must be a valid number")
			}
			if workers < 1 || workers > 16 {
				return fmt.Errorf("number of workers must be between 1 and 16")
			}
			return nil
		}),
		WithSubmitHandler(onSubmit),
	)
}
