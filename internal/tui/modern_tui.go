// internal/tui/modern_tui.go
package tui

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/storage"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/memory"
	"github.com/benjaminwestern/data-refinery/internal/state"
	"github.com/benjaminwestern/data-refinery/internal/tui/components"
)

// ModernTUI represents the new component-based TUI
type ModernTUI struct {
	componentManager *components.ComponentManager
	state            *components.SharedState
}

// NewModernTUI creates a new component-based TUI
func NewModernTUI(cfg *config.Config) (*ModernTUI, error) {
	cfg.GCSAvailable = testGCSClientModern()

	// Create shared state
	sharedState := &components.SharedState{
		Ctx:          context.Background(),
		ViewState:    components.ViewMenu,
		Config:       cfg,
		GCSAvailable: cfg.GCSAvailable,
	}

	// Initialize state manager with a default state directory
	stateManager, err := state.NewStateManager("./state", "modern-tui")
	if err != nil {
		return nil, fmt.Errorf("failed to create state manager: %w", err)
	}
	sharedState.StateManager = stateManager

	// Initialize memory manager with 1GB default limit
	memoryManager := memory.NewMemoryManager(1024) // 1GB in MB
	sharedState.MemoryManager = memoryManager

	// Start memory monitoring
	memoryManager.Start()

	// Create component manager
	componentManager := components.NewComponentManager(sharedState)

	// Register components
	componentManager.RegisterComponent(components.ViewMenu, components.NewMenuComponent(sharedState))
	componentManager.RegisterComponent(components.ViewProcessing, components.NewProcessingComponent(sharedState))

	return &ModernTUI{
		componentManager: componentManager,
		state:            sharedState,
	}, nil
}

// Run starts the modern TUI
func (m *ModernTUI) Run() (*config.Config, bool, bool, error) {
	// Start the TUI
	p := tea.NewProgram(m.componentManager, tea.WithAltScreen())

	if _, err := p.Run(); err != nil {
		return nil, false, false, fmt.Errorf("TUI error: %w", err)
	}

	// Clean up state manager
	if m.state.StateManager != nil {
		if err := m.state.StateManager.Close(); err != nil {
			log.Printf("Error closing state manager: %v", err)
		}
	}

	// Clean up memory manager
	if m.state.MemoryManager != nil {
		m.state.MemoryManager.Stop()
	}

	return m.state.Config, m.state.WantsToRestart, m.state.WantsToStartNew, nil
}

// testGCSClientModern tests if GCS is available (renamed to avoid conflicts)
func testGCSClientModern() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Printf("GCS client pre-flight check failed: %v. GCS functionality will be disabled.", err)
		return false
	}
	client.Close()
	return true
}

// GetCurrentState returns the current shared state
func (m *ModernTUI) GetCurrentState() *components.SharedState {
	return m.state
}

// SetViewState changes the current view state
func (m *ModernTUI) SetViewState(viewState components.ViewState) {
	m.state.ViewState = viewState
}

// GetComponentManager returns the component manager
func (m *ModernTUI) GetComponentManager() *components.ComponentManager {
	return m.componentManager
}
