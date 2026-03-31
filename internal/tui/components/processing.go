// Package components provides reusable TUI building blocks and shared state.
package components

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/benjaminwestern/data-refinery/internal/state"
)

var (
	statusStyle = lipgloss.NewStyle().MarginLeft(1).Foreground(primaryColor).Bold(true)
	timingStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("240"))
)

// ProcessingComponent handles the analysis processing view.
type ProcessingComponent struct {
	baseComponent
	state *SharedState
}

// NewProcessingComponent creates a new processing component.
func NewProcessingComponent(state *SharedState) *ProcessingComponent {
	return &ProcessingComponent{
		state: state,
	}
}

// Update handles messages for the processing component.
func (p *ProcessingComponent) Update(msg tea.Msg) (Component, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		// Only handle ctrl+c and q for cancellation
		switch msg.String() {
		case "ctrl+c", "q":
			return p, func() tea.Msg {
				return tea.KeyMsg{Type: tea.KeyCtrlC}
			}
		}
	}
	return p, nil
}

// View renders the processing component.
func (p *ProcessingComponent) View() string {
	var b strings.Builder

	b.WriteString(titleStyle.Render("🔍 Analysis in Progress"))
	b.WriteString("\n\n")

	// Show current status
	if p.state.Status != "" {
		b.WriteString(statusStyle.Render(p.state.Status))
		b.WriteString("\n\n")
	}

	// Show timing information
	if !p.state.StartTime.IsZero() {
		elapsed := time.Since(p.state.StartTime)
		if p.state.TotalElapsedTime > 0 {
			elapsed = p.state.TotalElapsedTime + elapsed
		}

		b.WriteString(timingStyle.Render(fmt.Sprintf("Elapsed: %v", elapsed.Round(time.Second))))

		if p.state.ETA > 0 {
			b.WriteString(timingStyle.Render(fmt.Sprintf(" | ETA: %v", p.state.ETA.Round(time.Second))))
		}
		b.WriteString("\n\n")
	}

	// Show processing statistics if available
	if currentState := p.currentAnalysisState(); currentState != nil {
		stats := currentState.ProcessingStats
		if stats.TotalBytesProcessed > 0 {
			fmt.Fprintf(&b, "Bytes processed: %d\n", stats.TotalBytesProcessed)
		}
		if stats.ProcessingRate > 0 {
			fmt.Fprintf(&b, "Processing rate: %.2f bytes/sec\n", stats.ProcessingRate)
		}
		if stats.TotalErrorsRecovered > 0 {
			b.WriteString(lipgloss.NewStyle().Foreground(lipgloss.Color("202")).Render(
				fmt.Sprintf("Errors recovered: %d\n", stats.TotalErrorsRecovered)))
		}
		if stats.WorkerUtilization > 0 {
			fmt.Fprintf(&b, "Worker utilization: %.1f%%\n", stats.WorkerUtilization*100)
		}
		b.WriteString("\n")
	}
	// Show spinner or progress if available
	if p.state.Analyser != nil {
		// This would need to be integrated with the actual analyser progress
		b.WriteString("⠋ Processing...")
		b.WriteString("\n\n")
	}

	b.WriteString(helpStyle.Render("Press Ctrl+C or q to cancel"))

	return b.String()
}

// Init initializes the processing component.
func (p *ProcessingComponent) Init() tea.Cmd {
	return nil
}

func (p *ProcessingComponent) currentAnalysisState() *state.AnalysisState {
	if p.state.StateManager == nil {
		return nil
	}

	return p.state.StateManager.GetCurrentState()
}
