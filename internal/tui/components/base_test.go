package components

import (
	"sync"
	"testing"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/memory"
	"github.com/benjaminwestern/data-refinery/internal/state"
	tea "github.com/charmbracelet/bubbletea"
)

// mockComponent implements Component interface for testing
type mockComponent struct {
	id        string
	focused   bool
	updateCnt int
	viewCnt   int
	initCnt   int
	focusCnt  int
	blurCnt   int
	mutex     sync.RWMutex
}

func (m *mockComponent) Init() tea.Cmd {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.initCnt++
	return nil
}

func (m *mockComponent) Update(msg tea.Msg) (Component, tea.Cmd) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.updateCnt++
	return m, nil
}

func (m *mockComponent) View() string {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.viewCnt++
	return m.id + "_view"
}

func (m *mockComponent) Focus() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.focused = true
	m.focusCnt++
}

func (m *mockComponent) Blur() {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.focused = false
	m.blurCnt++
}

func (m *mockComponent) IsFocused() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.focused
}

func (m *mockComponent) getCounts() (int, int, int, int, int) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.initCnt, m.updateCnt, m.viewCnt, m.focusCnt, m.blurCnt
}

func TestSharedStateCreation(t *testing.T) {
	memManager := memory.NewMemoryManager(1024) // 1GB
	stateManager, err := state.NewStateManager("./test_state", "test-session")
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer stateManager.Close()

	sharedState := &SharedState{
		MemoryManager: memManager,
		StateManager:  stateManager,
		ViewState:     ViewMenu,
		Config:        &config.Config{},
	}

	if sharedState == nil {
		t.Fatal("Expected shared state to be non-nil")
	}

	if sharedState.MemoryManager != memManager {
		t.Error("Expected memory manager to be set correctly")
	}

	if sharedState.StateManager != stateManager {
		t.Error("Expected state manager to be set correctly")
	}

	if sharedState.ViewState != ViewMenu {
		t.Error("Expected view state to be ViewMenu initially")
	}
}

func TestComponentManagerCreation(t *testing.T) {
	memManager := memory.NewMemoryManager(1024) // 1GB
	stateManager, err := state.NewStateManager("./test_state", "test-session")
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer stateManager.Close()
	sharedState := &SharedState{
		MemoryManager: memManager,
		StateManager:  stateManager,
		ViewState:     ViewMenu,
		Config:        &config.Config{},
	}

	manager := NewComponentManager(sharedState)

	if manager == nil {
		t.Fatal("Expected component manager to be non-nil")
	}

	if manager.state != sharedState {
		t.Error("Expected shared state to be set correctly")
	}

	if manager.components == nil {
		t.Error("Expected components map to be initialized")
	}

	if manager.state.ViewState != ViewMenu {
		t.Error("Expected view state to be ViewMenu initially")
	}
}

func TestComponentManagerRegisterComponent(t *testing.T) {
	memManager := memory.NewMemoryManager(1024) // 1GB
	stateManager, err := state.NewStateManager("./test_state", "test-session")
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer stateManager.Close()
	sharedState := &SharedState{
		MemoryManager: memManager,
		StateManager:  stateManager,
		ViewState:     ViewMenu,
		Config:        &config.Config{},
	}
	manager := NewComponentManager(sharedState)

	component := &mockComponent{id: "test_component"}

	// Register component
	manager.RegisterComponent(ViewMenu, component)

	// Verify component was registered
	if len(manager.components) != 1 {
		t.Errorf("Expected 1 component, got %d", len(manager.components))
	}

	if manager.components[ViewMenu] != component {
		t.Error("Expected component to be registered correctly")
	}
}

func TestComponentManagerView(t *testing.T) {
	memManager := memory.NewMemoryManager(1024) // 1GB
	stateManager, err := state.NewStateManager("./test_state", "test-session")
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer stateManager.Close()
	sharedState := &SharedState{
		MemoryManager: memManager,
		StateManager:  stateManager,
		ViewState:     ViewMenu,
		Config:        &config.Config{},
	}
	manager := NewComponentManager(sharedState)

	component := &mockComponent{id: "test_component"}
	manager.RegisterComponent(ViewMenu, component)

	// Get view
	view := manager.View()

	// Should contain component view
	if view != "test_component_view" {
		t.Errorf("Expected view to be 'test_component_view', got %s", view)
	}

	// Check that component view was called
	_, _, viewCnt, _, _ := component.getCounts()
	if viewCnt != 1 {
		t.Errorf("Expected component view to be called once, got %d", viewCnt)
	}
}

func TestComponentManagerUpdate(t *testing.T) {
	memManager := memory.NewMemoryManager(1024) // 1GB
	stateManager, err := state.NewStateManager("./test_state", "test-session")
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer stateManager.Close()
	sharedState := &SharedState{
		MemoryManager: memManager,
		StateManager:  stateManager,
		ViewState:     ViewMenu,
		Config:        &config.Config{},
	}
	manager := NewComponentManager(sharedState)

	component := &mockComponent{id: "test_component"}
	manager.RegisterComponent(ViewMenu, component)

	// Update manager with a message
	msg := tea.KeyMsg{Type: tea.KeyEnter}
	updatedManager, cmd := manager.Update(msg)

	// Should return updated manager
	if updatedManager == nil {
		t.Error("Expected updated manager to be non-nil")
	}

	// Command can be nil or valid
	if cmd != nil {
		t.Log("Update returned a command")
	}

	// Check that component was updated
	_, updateCnt, _, _, _ := component.getCounts()
	if updateCnt != 1 {
		t.Errorf("Expected component to be updated once, got %d", updateCnt)
	}
}

func TestMemoryManagerIntegration(t *testing.T) {
	memManager := memory.NewMemoryManager(1024) // 1GB
	stateManager, err := state.NewStateManager("./test_state", "test-session")
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer stateManager.Close()
	sharedState := &SharedState{
		MemoryManager: memManager,
		StateManager:  stateManager,
		ViewState:     ViewMenu,
		Config:        &config.Config{},
	}

	// Test memory manager integration
	stats := memManager.GetStats()
	if stats.MaxMemoryMB != 1024 {
		t.Errorf("Expected memory limit to be 1024MB, got %d", stats.MaxMemoryMB)
	}

	// Test with shared state
	if sharedState.MemoryManager != memManager {
		t.Error("Expected memory manager to be accessible from shared state")
	}
}

func TestComponentManagerConcurrentAccess(t *testing.T) {
	memManager := memory.NewMemoryManager(1024) // 1GB
	stateManager, err := state.NewStateManager("./test_state", "test-session")
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}
	defer stateManager.Close()
	sharedState := &SharedState{
		MemoryManager: memManager,
		StateManager:  stateManager,
		ViewState:     ViewMenu,
		Config:        &config.Config{},
	}
	manager := NewComponentManager(sharedState)

	component1 := &mockComponent{id: "component1"}
	component2 := &mockComponent{id: "component2"}

	manager.RegisterComponent(ViewMenu, component1)
	manager.RegisterComponent(ViewOptions, component2)

	// Simulate concurrent access
	var wg sync.WaitGroup

	// Concurrent updates
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := tea.KeyMsg{Type: tea.KeyEnter}
			manager.Update(msg)
		}()
	}

	// Concurrent views
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			manager.View()
		}()
	}

	wg.Wait()

	// Should not crash and should have valid state
	if manager.state.ViewState != ViewMenu {
		t.Error("Expected view state to remain ViewMenu")
	}
}

func BenchmarkComponentManagerOperations(b *testing.B) {
	memManager := memory.NewMemoryManager(1024) // 1GB
	stateManager, err := state.NewStateManager("./test_state", "test-session")
	if err != nil {
		b.Fatalf("Failed to create state manager: %v", err)
	}
	defer stateManager.Close()
	sharedState := &SharedState{
		MemoryManager: memManager,
		StateManager:  stateManager,
		ViewState:     ViewMenu,
		Config:        &config.Config{},
	}
	manager := NewComponentManager(sharedState)

	// Register multiple components
	viewStates := []ViewState{ViewMenu, ViewOptions, ViewHelp, ViewInputPath, ViewProcessing}
	for _, vs := range viewStates {
		component := &mockComponent{id: string(rune('a' + int(vs)))}
		manager.RegisterComponent(vs, component)
	}

	b.ResetTimer()

	b.Run("Update", func(b *testing.B) {
		msg := tea.KeyMsg{Type: tea.KeyEnter}
		for i := 0; i < b.N; i++ {
			manager.Update(msg)
		}
	})

	b.Run("View", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			manager.View()
		}
	})

	b.Run("GetComponent", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			manager.GetComponent(ViewMenu)
		}
	})
}
