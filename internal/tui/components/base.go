// Package components provides reusable TUI building blocks and shared state.
package components

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/benjaminwestern/data-refinery/internal/analyser"
	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/memory"
	"github.com/benjaminwestern/data-refinery/internal/report"
	"github.com/benjaminwestern/data-refinery/internal/source"
	"github.com/benjaminwestern/data-refinery/internal/state"
)

// Styles - matching the ones from the original TUI.
var (
	primaryColor = lipgloss.Color("63")
	errorColor   = lipgloss.Color("196")
	spinnerStyle = lipgloss.NewStyle().Foreground(primaryColor)
	errorStyle   = lipgloss.NewStyle().Foreground(errorColor).Bold(true)
)

// ViewState represents the current view state.
type ViewState int

// ViewState values describe the primary screens available in the components package.
const (
	ViewMenu ViewState = iota
	ViewOptions
	ViewHelp
	ViewInputPath
	ViewInputKey
	ViewInputLogPath
	ViewProcessing
	ViewCancelling
	ViewReport
	ViewPurgeSelection
	ViewPurging
	ViewAdvancedMenu
	ViewSchemaConfig
	ViewSchemaResult
	ViewSearchConfig
	ViewSearchTargetEdit
	ViewHashingConfig
	ViewDeletionConfig
	ViewDeletionRuleEdit
	ViewAdvancedOutput
)

// Component represents a UI component with state and rendering capability.
type Component interface {
	// Update handles messages and returns updated component and commands
	Update(msg tea.Msg) (Component, tea.Cmd)

	// View renders the component to a string
	View() string

	// Init initializes the component
	Init() tea.Cmd

	// Focus sets focus to the component
	Focus()

	// Blur removes focus from the component
	Blur()

	// IsFocused returns whether the component is focused
	IsFocused() bool
}

// baseComponent provides common functionality for all components.
type baseComponent struct {
	focused bool
	width   int
	height  int
}

func (c *baseComponent) Init() tea.Cmd {
	return nil
}

func (c *baseComponent) Focus() {
	c.focused = true
}

func (c *baseComponent) Blur() {
	c.focused = false
}

func (c *baseComponent) IsFocused() bool {
	return c.focused
}

func (c *baseComponent) SetSize(width, height int) {
	c.width = width
	c.height = height
}

func (c *baseComponent) Width() int {
	return c.width
}

func (c *baseComponent) Height() int {
	return c.height
}

// SharedState contains state shared across all components.
type SharedState struct {
	// Core application state
	Ctx             context.Context
	JobCtx          context.Context
	JobCancel       context.CancelFunc
	Analyser        *analyser.Analyser
	StateManager    *state.Manager
	MemoryManager   *memory.Manager
	OriginalSources []source.InputSource

	// UI state
	ViewState       ViewState
	Quitting        bool
	Error           error
	Status          string
	WantsToRestart  bool
	WantsToStartNew bool
	GCSAvailable    bool

	// Processing state
	Processing       bool
	WasCancelled     bool
	IsValidationRun  bool
	StartTime        time.Time
	TotalElapsedTime time.Duration
	ETA              time.Duration
	FinalReport      *report.AnalysisReport
	SavedFilename    string

	// Configuration
	Config *config.Config

	// Advanced features
	AdvancedEnabled bool
	SearchTargets   []config.SearchTarget
	HashingStrategy config.HashingStrategy
	DeletionRules   []config.DeletionRule
	SchemaDiscovery config.SchemaDiscoveryConfig

	// Purge state
	PurgeIDKeys     []string
	PurgeRowHashes  []string
	RecordsToDelete map[string]map[int]bool
	PurgeStats      PurgeResultMsg
}

// PurgeResultMsg represents purge operation results.
type PurgeResultMsg struct {
	Success        bool
	DeletedFiles   int
	DeletedRecords int
	Error          error
	BackupLocation string
}

// ComponentManager manages multiple components and their interactions.
type ComponentManager struct {
	mu         sync.RWMutex
	components map[ViewState]Component
	state      *SharedState
	spinner    spinner.Model
	progress   progress.Model

	// Common inputs used across components
	pathInput    textinput.Model
	keyInput     textinput.Model
	logPathInput textinput.Model
}

// NewComponentManager creates a new component manager.
func NewComponentManager(state *SharedState) *ComponentManager {
	cm := &ComponentManager{
		components: make(map[ViewState]Component),
		state:      state,
		spinner:    spinner.New(),
		progress:   progress.New(progress.WithDefaultGradient()),
	}

	// Initialize common inputs
	cm.pathInput = textinput.New()
	cm.pathInput.Placeholder = "Enter path to data source..."
	cm.pathInput.Width = 50
	cm.pathInput.CharLimit = 200

	cm.keyInput = textinput.New()
	cm.keyInput.Placeholder = "Enter GCS service account key path..."
	cm.keyInput.Width = 50
	cm.keyInput.CharLimit = 200
	cm.keyInput.EchoMode = textinput.EchoPassword

	cm.logPathInput = textinput.New()
	cm.logPathInput.Placeholder = "Enter log output path..."
	cm.logPathInput.Width = 50
	cm.logPathInput.CharLimit = 200

	// Set up spinner
	cm.spinner.Spinner = spinner.Dot
	cm.spinner.Style = spinnerStyle

	return cm
}

// RegisterComponent registers a component for a specific view state.
func (cm *ComponentManager) RegisterComponent(viewState ViewState, component Component) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.components[viewState] = component
}

// GetComponent returns the component for a specific view state.
func (cm *ComponentManager) GetComponent(viewState ViewState) Component {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.components[viewState]
}

// Update handles messages and delegates to the appropriate component.
func (cm *ComponentManager) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	// Handle global messages first
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		cm.mu.Lock()
		defer cm.mu.Unlock()
		cm.progress.Width = msg.Width - 4
		for _, component := range cm.components {
			if sizable, ok := component.(interface{ SetSize(int, int) }); ok {
				sizable.SetSize(msg.Width, msg.Height)
			}
		}
		return cm, nil

	case tea.KeyMsg:
		if cm.state.Error != nil {
			cm.state.Error = nil
			cm.state.ViewState = ViewMenu
			return cm, nil
		}

		if msg.String() == "ctrl+c" || msg.String() == "q" {
			return cm.handleQuit()
		}

		if msg.Type == tea.KeyEsc {
			return cm.handleEscape()
		}
	}

	// Delegate to current component
	if component := cm.GetComponent(cm.state.ViewState); component != nil {
		updatedComponent, cmd := component.Update(msg)
		cm.mu.Lock()
		defer cm.mu.Unlock()
		cm.components[cm.state.ViewState] = updatedComponent
		return cm, cmd
	}

	return cm, nil
}

// View renders the current view.
func (cm *ComponentManager) View() string {
	if cm.state.Error != nil {
		return cm.renderError()
	}

	if component := cm.GetComponent(cm.state.ViewState); component != nil {
		return component.View()
	}

	return "Unknown view state"
}

// handleQuit handles quit requests.
func (cm *ComponentManager) handleQuit() (tea.Model, tea.Cmd) {
	if cm.state.ViewState == ViewProcessing {
		cm.state.Status = "Cancelling... generating partial report."
		cm.state.ViewState = ViewCancelling
		cm.state.WasCancelled = true
		if !cm.state.StartTime.IsZero() {
			cm.state.TotalElapsedTime += time.Since(cm.state.StartTime)
			cm.state.StartTime = time.Time{}
		}
		if cm.state.JobCancel != nil {
			cm.state.JobCancel()
		}
		return cm, nil
	}

	if cm.state.ViewState == ViewCancelling || cm.state.ViewState == ViewPurging {
		return cm, nil
	}

	cm.state.Quitting = true
	if cm.state.JobCancel != nil {
		cm.state.JobCancel()
	}
	return cm, tea.Quit
}

// handleEscape handles escape key presses.
func (cm *ComponentManager) handleEscape() (tea.Model, tea.Cmd) {
	switch cm.state.ViewState {
	case ViewMenu, ViewProcessing, ViewCancelling, ViewPurging:
		return cm, nil
	case ViewHelp, ViewOptions, ViewInputPath, ViewReport:
		cm.state.ViewState = ViewMenu
		return cm, nil
	case ViewAdvancedMenu, ViewSchemaConfig, ViewSearchConfig, ViewHashingConfig, ViewDeletionConfig, ViewAdvancedOutput:
		cm.state.ViewState = ViewAdvancedMenu
		return cm, nil
	case ViewSearchTargetEdit:
		cm.state.ViewState = ViewSearchConfig
		return cm, nil
	case ViewDeletionRuleEdit:
		cm.state.ViewState = ViewDeletionConfig
		return cm, nil
	case ViewSchemaResult:
		cm.state.ViewState = ViewSchemaConfig
		return cm, nil
	case ViewInputKey:
		cm.state.ViewState = ViewInputPath
		return cm, nil
	case ViewInputLogPath:
		cm.state.ViewState = ViewOptions
		return cm, nil
	case ViewPurgeSelection:
		cm.state.ViewState = ViewReport
		return cm, nil
	}

	return cm, nil
}

// renderError renders error messages.
func (cm *ComponentManager) renderError() string {
	return errorStyle.Render(fmt.Sprintf("Error: %v\n\nPress any key to continue...", cm.state.Error))
}

// Init initializes the component manager.
func (cm *ComponentManager) Init() tea.Cmd {
	return tea.Batch(
		cm.spinner.Tick,
		tea.EnterAltScreen,
	)
}

// GetSharedState returns the shared state.
func (cm *ComponentManager) GetSharedState() *SharedState {
	return cm.state
}

// GetSpinner returns the spinner model.
func (cm *ComponentManager) GetSpinner() spinner.Model {
	return cm.spinner
}

// GetProgress returns the progress model.
func (cm *ComponentManager) GetProgress() progress.Model {
	return cm.progress
}

// GetPathInput returns the path input model.
func (cm *ComponentManager) GetPathInput() textinput.Model {
	return cm.pathInput
}

// GetKeyInput returns the key input model.
func (cm *ComponentManager) GetKeyInput() textinput.Model {
	return cm.keyInput
}

// GetLogPathInput returns the log path input model.
func (cm *ComponentManager) GetLogPathInput() textinput.Model {
	return cm.logPathInput
}
