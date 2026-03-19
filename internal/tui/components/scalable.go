package components

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/benjaminwestern/dupe-analyser/internal/tui/layout"
)

// ScalableComponent represents a component that can adapt to different terminal sizes
type ScalableComponent interface {
	// Update the component for new terminal dimensions
	UpdateSize(layout *layout.ResponsiveLayout)
	// Render the component with current size constraints
	Render(layout *layout.ResponsiveLayout) string
}

// ScalableProgress is a progress bar that adapts to terminal size
type ScalableProgress struct {
	progress progress.Model
	label    string
	percent  float64
	details  string
}

// NewScalableProgress creates a new adaptive progress bar
func NewScalableProgress(label string) *ScalableProgress {
	p := progress.New(progress.WithDefaultGradient())
	return &ScalableProgress{
		progress: p,
		label:    label,
		percent:  0.0,
	}
}

// UpdateSize adjusts the progress bar for new terminal dimensions
func (sp *ScalableProgress) UpdateSize(layout *layout.ResponsiveLayout) {
	constraints := layout.GetConstraints()

	// Calculate appropriate width based on terminal size
	width := constraints.ContentWidth - 10
	if width > 120 {
		width = 120
	}
	if width < 20 {
		width = 20
	}

	sp.progress.Width = width
}

// SetPercent updates the progress percentage
func (sp *ScalableProgress) SetPercent(percent float64) {
	sp.percent = percent
}

// SetDetails updates the progress details text
func (sp *ScalableProgress) SetDetails(details string) {
	sp.details = details
}

// Update handles progress bar updates
func (sp *ScalableProgress) Update(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	progressModel, cmd := sp.progress.Update(msg)
	if pm, ok := progressModel.(progress.Model); ok {
		sp.progress = pm
	}
	return cmd
}

// Render displays the progress bar with current size constraints
func (sp *ScalableProgress) Render(layout *layout.ResponsiveLayout) string {
	constraints := layout.GetConstraints()
	styles := layout.GetStyles()

	var content strings.Builder

	// Label
	if sp.label != "" {
		labelStyle := styles.Subtitle
		if constraints.IsNarrow {
			labelStyle = labelStyle.Align(lipgloss.Left)
		} else {
			labelStyle = labelStyle.Align(lipgloss.Center)
		}
		content.WriteString(labelStyle.Render(sp.label) + "\n")
	}

	// Progress bar
	sp.progress.SetPercent(sp.percent)
	progressView := sp.progress.View()

	if constraints.IsNarrow {
		// Simple progress for narrow terminals
		content.WriteString(progressView + "\n")
	} else {
		// Centered progress for wider terminals
		progressStyle := lipgloss.NewStyle().
			Width(constraints.ContentWidth).
			Align(lipgloss.Center)
		content.WriteString(progressStyle.Render(progressView) + "\n")
	}

	// Details
	if sp.details != "" {
		detailsStyle := styles.Description
		if constraints.IsNarrow {
			detailsStyle = detailsStyle.Width(constraints.ContentWidth)
		} else {
			detailsStyle = detailsStyle.Width(constraints.MaxCardWidth).
				Align(lipgloss.Center)
		}
		content.WriteString(detailsStyle.Render(sp.details) + "\n")
	}

	return content.String()
}

// ScalableSpinner is a spinner that adapts to terminal size
type ScalableSpinner struct {
	spinner spinner.Model
	label   string
	timing  time.Duration
}

// NewScalableSpinner creates a new adaptive spinner
func NewScalableSpinner(label string) *ScalableSpinner {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("63"))

	return &ScalableSpinner{
		spinner: s,
		label:   label,
	}
}

// UpdateSize adjusts the spinner for new terminal dimensions
func (ss *ScalableSpinner) UpdateSize(layout *layout.ResponsiveLayout) {
	// Spinner doesn't need size adjustments, but we can modify style
	constraints := layout.GetConstraints()

	if constraints.IsNarrow {
		// Use simpler spinner for narrow terminals
		ss.spinner.Spinner = spinner.Line
	} else {
		// Use more elaborate spinner for wider terminals
		ss.spinner.Spinner = spinner.Dot
	}
}

// SetTiming updates the timing information
func (ss *ScalableSpinner) SetTiming(timing time.Duration) {
	ss.timing = timing
}

// Update handles spinner updates
func (ss *ScalableSpinner) Update(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	ss.spinner, cmd = ss.spinner.Update(msg)
	return cmd
}

// Render displays the spinner with current size constraints
func (ss *ScalableSpinner) Render(layout *layout.ResponsiveLayout) string {
	constraints := layout.GetConstraints()
	styles := layout.GetStyles()

	var content strings.Builder

	// Spinner and label
	spinnerView := ss.spinner.View()
	labelText := ss.label

	if constraints.IsNarrow {
		// Compact format for narrow terminals
		content.WriteString(fmt.Sprintf("%s %s", spinnerView, labelText))
	} else {
		// Spacious format for wider terminals
		statusStyle := styles.Subtitle.Foreground(lipgloss.Color("63"))
		content.WriteString(fmt.Sprintf("%s %s", spinnerView, statusStyle.Render(labelText)))
	}

	// Timing information
	if ss.timing > 0 {
		timingText := fmt.Sprintf(" (Elapsed: %s)", ss.timing.Round(time.Second))
		if constraints.IsNarrow {
			content.WriteString("\n" + timingText)
		} else {
			timingStyle := styles.Description
			content.WriteString(timingStyle.Render(timingText))
		}
	}

	return content.String()
}

// ScalableTextInput is a text input that adapts to terminal size
type ScalableTextInput struct {
	input       textinput.Model
	label       string
	placeholder string
	help        string
}

// NewScalableTextInput creates a new adaptive text input
func NewScalableTextInput(label, placeholder, help string) *ScalableTextInput {
	input := textinput.New()
	input.Placeholder = placeholder

	return &ScalableTextInput{
		input:       input,
		label:       label,
		placeholder: placeholder,
		help:        help,
	}
}

// UpdateSize adjusts the text input for new terminal dimensions
func (sti *ScalableTextInput) UpdateSize(layout *layout.ResponsiveLayout) {
	constraints := layout.GetConstraints()

	// Calculate appropriate width based on terminal size
	width := constraints.MaxCardWidth - 6
	if constraints.IsNarrow {
		width = constraints.ContentWidth - 4
	}

	if width < 20 {
		width = 20
	}

	sti.input.Width = width
}

// SetValue sets the input value
func (sti *ScalableTextInput) SetValue(value string) {
	sti.input.SetValue(value)
}

// GetValue returns the current input value
func (sti *ScalableTextInput) GetValue() string {
	return sti.input.Value()
}

// Focus focuses the input
func (sti *ScalableTextInput) Focus() {
	sti.input.Focus()
}

// Blur blurs the input
func (sti *ScalableTextInput) Blur() {
	sti.input.Blur()
}

// Update handles text input updates
func (sti *ScalableTextInput) Update(msg tea.Msg) tea.Cmd {
	var cmd tea.Cmd
	sti.input, cmd = sti.input.Update(msg)
	return cmd
}

// Render displays the text input with current size constraints
func (sti *ScalableTextInput) Render(layout *layout.ResponsiveLayout) string {
	constraints := layout.GetConstraints()
	styles := layout.GetStyles()

	var content strings.Builder

	// Label
	if sti.label != "" {
		labelStyle := styles.Subtitle
		content.WriteString(labelStyle.Render(sti.label) + "\n")
	}

	// Input box
	inputView := sti.input.View()
	inputStyle := styles.Input

	if constraints.IsNarrow {
		// Full width for narrow terminals
		inputStyle = inputStyle.Width(constraints.ContentWidth - 4)
	} else {
		// Constrained width for wider terminals
		inputStyle = inputStyle.Width(constraints.MaxCardWidth - 6)
	}

	content.WriteString(inputStyle.Render(inputView) + "\n")

	// Help text
	if sti.help != "" {
		helpStyle := styles.Help
		if constraints.IsNarrow {
			helpStyle = helpStyle.Width(constraints.ContentWidth)
		} else {
			helpStyle = helpStyle.Width(constraints.MaxCardWidth)
		}
		content.WriteString(helpStyle.Render(sti.help) + "\n")
	}

	return content.String()
}

// ScalableMenu is a menu that adapts to terminal size
type ScalableMenu struct {
	title       string
	description string
	items       []layout.MenuItem
	cursor      int
	helpText    string
}

// NewScalableMenu creates a new adaptive menu
func NewScalableMenu(title, description string) *ScalableMenu {
	return &ScalableMenu{
		title:       title,
		description: description,
		items:       []layout.MenuItem{},
		cursor:      0,
	}
}

// AddItem adds a menu item
func (sm *ScalableMenu) AddItem(title, description, status string, enabled bool) {
	sm.items = append(sm.items, layout.MenuItem{
		Title:       title,
		Description: description,
		Status:      status,
		Selected:    false,
		Enabled:     enabled,
	})
}

// SetCursor sets the current cursor position
func (sm *ScalableMenu) SetCursor(cursor int) {
	if cursor >= 0 && cursor < len(sm.items) {
		sm.cursor = cursor
	}
}

// GetCursor returns the current cursor position
func (sm *ScalableMenu) GetCursor() int {
	return sm.cursor
}

// GetSelectedItem returns the currently selected item
func (sm *ScalableMenu) GetSelectedItem() *layout.MenuItem {
	if sm.cursor >= 0 && sm.cursor < len(sm.items) {
		return &sm.items[sm.cursor]
	}
	return nil
}

// SetHelpText sets the help text
func (sm *ScalableMenu) SetHelpText(help string) {
	sm.helpText = help
}

// UpdateSize adjusts the menu for new terminal dimensions
func (sm *ScalableMenu) UpdateSize(layout *layout.ResponsiveLayout) {
	// Menu doesn't need specific size adjustments
	// The layout system handles the formatting
}

// Render displays the menu with current size constraints
func (sm *ScalableMenu) Render(layout *layout.ResponsiveLayout) string {
	constraints := layout.GetConstraints()
	styles := layout.GetStyles()

	var content strings.Builder

	// Title
	if sm.title != "" {
		titleStyle := styles.Title
		if constraints.IsWide {
			titleStyle = titleStyle.Align(lipgloss.Center)
		}
		content.WriteString(titleStyle.Render(sm.title) + "\n")
	}

	// Description
	if sm.description != "" {
		descStyle := styles.Description
		if constraints.IsWide {
			descStyle = descStyle.Align(lipgloss.Center)
		}
		content.WriteString(descStyle.Render(sm.description) + "\n\n")
	}

	// Update selection state
	for i := range sm.items {
		sm.items[i].Selected = (i == sm.cursor)
	}

	// Menu items
	menuContent := layout.FormatMenu(sm.items)
	content.WriteString(menuContent + "\n")

	// Help text
	if sm.helpText != "" {
		helpStyle := styles.Help
		if constraints.IsNarrow {
			helpStyle = helpStyle.Width(constraints.ContentWidth)
		} else {
			helpStyle = helpStyle.Width(constraints.MaxCardWidth)
		}
		content.WriteString(helpStyle.Render(sm.helpText))
	}

	return content.String()
}

// ScalableCard is a card container that adapts to terminal size
type ScalableCard struct {
	title   string
	content string
	footer  string
}

// NewScalableCard creates a new adaptive card
func NewScalableCard(title, content, footer string) *ScalableCard {
	return &ScalableCard{
		title:   title,
		content: content,
		footer:  footer,
	}
}

// SetContent updates the card content
func (sc *ScalableCard) SetContent(content string) {
	sc.content = content
}

// UpdateSize adjusts the card for new terminal dimensions
func (sc *ScalableCard) UpdateSize(layout *layout.ResponsiveLayout) {
	// Card adapts automatically through the layout system
}

// Render displays the card with current size constraints
func (sc *ScalableCard) Render(layout *layout.ResponsiveLayout) string {
	constraints := layout.GetConstraints()
	styles := layout.GetStyles()

	var content strings.Builder

	// Title
	if sc.title != "" {
		titleStyle := styles.Subtitle
		if constraints.IsWide {
			titleStyle = titleStyle.Align(lipgloss.Center)
		}
		content.WriteString(titleStyle.Render(sc.title) + "\n\n")
	}

	// Content
	contentStyle := lipgloss.NewStyle()
	if constraints.IsNarrow {
		contentStyle = contentStyle.Width(constraints.ContentWidth)
	} else {
		contentStyle = contentStyle.Width(constraints.MaxCardWidth)
	}
	content.WriteString(contentStyle.Render(sc.content))

	// Footer
	if sc.footer != "" {
		content.WriteString("\n\n")
		footerStyle := styles.Help
		if constraints.IsNarrow {
			footerStyle = footerStyle.Width(constraints.ContentWidth)
		} else {
			footerStyle = footerStyle.Width(constraints.MaxCardWidth)
		}
		content.WriteString(footerStyle.Render(sc.footer))
	}

	// Wrap in card style
	cardStyle := styles.Card
	if constraints.IsNarrow {
		cardStyle = cardStyle.Width(constraints.ContentWidth)
	} else {
		cardStyle = cardStyle.Width(constraints.MaxCardWidth)
	}

	return cardStyle.Render(content.String())
}

// ScalableReport is a report display that adapts to terminal size
type ScalableReport struct {
	title    string
	sections []ReportSection
	summary  string
	actions  []string
}

// ReportSection represents a section in the report
type ReportSection struct {
	Title   string
	Content string
	Items   []string
}

// NewScalableReport creates a new adaptive report
func NewScalableReport(title string) *ScalableReport {
	return &ScalableReport{
		title:    title,
		sections: []ReportSection{},
		actions:  []string{},
	}
}

// AddSection adds a report section
func (sr *ScalableReport) AddSection(title, content string, items []string) {
	sr.sections = append(sr.sections, ReportSection{
		Title:   title,
		Content: content,
		Items:   items,
	})
}

// SetSummary sets the report summary
func (sr *ScalableReport) SetSummary(summary string) {
	sr.summary = summary
}

// SetActions sets the available actions
func (sr *ScalableReport) SetActions(actions []string) {
	sr.actions = actions
}

// UpdateSize adjusts the report for new terminal dimensions
func (sr *ScalableReport) UpdateSize(layout *layout.ResponsiveLayout) {
	// Report adapts automatically through the layout system
}

// Render displays the report with current size constraints
func (sr *ScalableReport) Render(layout *layout.ResponsiveLayout) string {
	constraints := layout.GetConstraints()
	styles := layout.GetStyles()

	var content strings.Builder

	// Title
	if sr.title != "" {
		titleStyle := styles.Title
		if constraints.IsWide {
			titleStyle = titleStyle.Align(lipgloss.Center)
		}
		content.WriteString(titleStyle.Render(sr.title) + "\n\n")
	}

	// Sections
	for i, section := range sr.sections {
		// Section title
		sectionTitleStyle := styles.Subtitle
		content.WriteString(sectionTitleStyle.Render(section.Title) + "\n")

		// Section content
		if section.Content != "" {
			contentStyle := lipgloss.NewStyle()
			if constraints.IsNarrow {
				contentStyle = contentStyle.Width(constraints.ContentWidth)
			} else {
				contentStyle = contentStyle.Width(constraints.MaxCardWidth)
			}
			content.WriteString(contentStyle.Render(section.Content) + "\n")
		}

		// Section items
		for _, item := range section.Items {
			itemStyle := lipgloss.NewStyle().
				Foreground(lipgloss.Color("240")).
				MarginLeft(2)
			if constraints.IsNarrow {
				itemStyle = itemStyle.Width(constraints.ContentWidth - 4)
			} else {
				itemStyle = itemStyle.Width(constraints.MaxCardWidth - 4)
			}
			content.WriteString(itemStyle.Render("• "+item) + "\n")
		}

		if i < len(sr.sections)-1 {
			content.WriteString("\n")
		}
	}

	// Summary
	if sr.summary != "" {
		content.WriteString("\n\n")
		summaryStyle := styles.Card
		if constraints.IsNarrow {
			summaryStyle = summaryStyle.Width(constraints.ContentWidth)
		} else {
			summaryStyle = summaryStyle.Width(constraints.MaxCardWidth)
		}
		content.WriteString(summaryStyle.Render(sr.summary))
	}

	// Actions
	if len(sr.actions) > 0 {
		content.WriteString("\n\n")
		actionsText := "Available actions: " + strings.Join(sr.actions, ", ")
		actionsStyle := styles.Help
		if constraints.IsNarrow {
			actionsStyle = actionsStyle.Width(constraints.ContentWidth)
		} else {
			actionsStyle = actionsStyle.Width(constraints.MaxCardWidth)
		}
		content.WriteString(actionsStyle.Render(actionsText))
	}

	return content.String()
}
