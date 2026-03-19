// internal/tui/components/menu.go
package components

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

var (
	menuCursorStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("212")).Bold(true)
	menuItemStyle   = lipgloss.NewStyle().PaddingLeft(2)
	titleStyle      = lipgloss.NewStyle().Bold(true).Foreground(primaryColor).MarginBottom(1)
	helpStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Italic(true).Margin(1, 0)
)

// MenuComponent handles the main menu interface
type MenuComponent struct {
	BaseComponent
	state  *SharedState
	cursor int
	items  []MenuItem
}

// MenuItem represents a menu item
type MenuItem struct {
	Label       string
	Description string
	Action      func() tea.Cmd
}

// NewMenuComponent creates a new menu component
func NewMenuComponent(state *SharedState) *MenuComponent {
	menu := &MenuComponent{
		state:  state,
		cursor: 0,
		items: []MenuItem{
			{
				Label:       "Start New Analysis",
				Description: "Configure and start duplicate analysis",
				Action: func() tea.Cmd {
					state.ViewState = ViewInputPath
					return nil
				},
			},
			{
				Label:       "Resume Previous Analysis",
				Description: "Continue from saved state",
				Action: func() tea.Cmd {
					// Check if state manager exists and has saved states
					if state.StateManager != nil {
						states, err := state.StateManager.ListStates()
						if err == nil && len(states) > 0 {
							// Resume the most recent state
							latestState := states[0]
							if _, err := state.StateManager.LoadState(latestState); err == nil {
								state.ViewState = ViewProcessing
								return nil
							}
						}
					}
					// If no state available, show error or fallback
					return nil
				},
			},
			{
				Label:       "Options",
				Description: "Configure analysis parameters",
				Action: func() tea.Cmd {
					state.ViewState = ViewOptions
					return nil
				},
			},
			{
				Label:       "Advanced Features",
				Description: "Schema discovery, search, deletion rules",
				Action: func() tea.Cmd {
					state.ViewState = ViewAdvancedMenu
					return nil
				},
			},
			{
				Label:       "Help",
				Description: "View help and usage information",
				Action: func() tea.Cmd {
					state.ViewState = ViewHelp
					return nil
				},
			},
			{
				Label:       "Quit",
				Description: "Exit the application",
				Action: func() tea.Cmd {
					state.Quitting = true
					return tea.Quit
				},
			},
		},
	}
	return menu
}

// Update handles messages for the menu component
func (m *MenuComponent) Update(msg tea.Msg) (Component, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.cursor > 0 {
				m.cursor--
			}
		case "down", "j":
			if m.cursor < len(m.items)-1 {
				m.cursor++
			}
		case "enter", " ":
			if m.cursor < len(m.items) {
				return m, m.items[m.cursor].Action()
			}
		}
	}
	return m, nil
}

// View renders the menu component
func (m *MenuComponent) View() string {
	var b strings.Builder

	// Title
	b.WriteString(titleStyle.Render("🔍 Data Refinery"))
	b.WriteString("\n\n")

	// Status information
	if m.state.StateManager != nil {
		// Check if we have saved state
		if states, err := m.state.StateManager.ListStates(); err == nil && len(states) > 0 {
			b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("46")).Render("💾 Saved state available"))
			b.WriteString("\n")
		}
	}

	// GCS availability
	if m.state.GCSAvailable {
		b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("46")).Render("✓ GCS available"))
	} else {
		b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("202")).Render("⚠ GCS unavailable"))
	}
	b.WriteString("\n\n")

	// Menu items
	for i, item := range m.items {
		cursor := "  "
		if i == m.cursor {
			cursor = menuCursorStyle.Render("> ")
		}

		line := fmt.Sprintf("%s%s", cursor, item.Label)
		if i == m.cursor {
			line = menuCursorStyle.Render(line)
		}

		b.WriteString(line)
		b.WriteString("\n")

		// Show description for selected item
		if i == m.cursor && item.Description != "" {
			description := menuItemStyle.Render(
				lipgloss.NewStyle().Foreground(lipgloss.Color("240")).Render(item.Description),
			)
			b.WriteString(description)
			b.WriteString("\n")
		}
	}

	b.WriteString("\n")
	b.WriteString(helpStyle.Render("Use ↑/↓ or j/k to navigate, enter to select, q to quit"))

	return b.String()
}

// Init initializes the menu component
func (m *MenuComponent) Init() tea.Cmd {
	return nil
}

// GetCursor returns the current cursor position
func (m *MenuComponent) GetCursor() int {
	return m.cursor
}

// SetCursor sets the cursor position
func (m *MenuComponent) SetCursor(cursor int) {
	if cursor >= 0 && cursor < len(m.items) {
		m.cursor = cursor
	}
}
