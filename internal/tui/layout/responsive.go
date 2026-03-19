package layout

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// LayoutConstraints defines the responsive layout system boundaries
type LayoutConstraints struct {
	// Terminal dimensions
	Width  int
	Height int

	// Breakpoints for responsive behavior
	IsNarrow bool // < 80 columns
	IsWide   bool // >= 120 columns
	IsTall   bool // >= 40 rows
	IsShort  bool // < 20 rows

	// Content sizing
	ContentWidth  int
	ContentHeight int
	SidebarWidth  int
	MaxCardWidth  int
}

// ResponsiveLayout manages adaptive UI layout based on terminal size
type ResponsiveLayout struct {
	constraints LayoutConstraints
	styles      ResponsiveStyles
}

// ResponsiveStyles contains size-adapted styling
type ResponsiveStyles struct {
	// Basic styles
	Title       lipgloss.Style
	Subtitle    lipgloss.Style
	Description lipgloss.Style
	Help        lipgloss.Style
	Error       lipgloss.Style

	// Container styles
	Card     lipgloss.Style
	Input    lipgloss.Style
	Menu     lipgloss.Style
	Report   lipgloss.Style
	Progress lipgloss.Style

	// Layout styles
	Header  lipgloss.Style
	Content lipgloss.Style
	Sidebar lipgloss.Style
	Footer  lipgloss.Style
}

// NewResponsiveLayout creates a new responsive layout system
func NewResponsiveLayout(width, height int) *ResponsiveLayout {
	constraints := calculateConstraints(width, height)
	styles := createResponsiveStyles(constraints)

	return &ResponsiveLayout{
		constraints: constraints,
		styles:      styles,
	}
}

// calculateConstraints determines layout boundaries based on terminal size
func calculateConstraints(width, height int) LayoutConstraints {
	// Handle edge cases
	if width <= 0 {
		width = 80
	}
	if height <= 0 {
		height = 24
	}

	// Calculate breakpoints
	isNarrow := width < 80
	isWide := width >= 120
	isTall := height >= 40
	isShort := height < 20

	// Calculate content dimensions
	contentWidth := width - 8 // Account for margins and padding
	if contentWidth < 40 {
		contentWidth = width - 2 // Minimal padding for very narrow terminals
	}

	contentHeight := height - 4 // Account for header/footer
	if contentHeight < 10 {
		contentHeight = height - 2 // Minimal padding for very short terminals
	}

	// Sidebar width based on available space
	sidebarWidth := 30
	if isNarrow {
		sidebarWidth = 0 // No sidebar on narrow terminals
	} else if width < 100 {
		sidebarWidth = 20
	}

	// Maximum card width for readability
	maxCardWidth := 100
	if isNarrow {
		maxCardWidth = contentWidth
	} else if contentWidth < maxCardWidth {
		maxCardWidth = contentWidth
	}

	return LayoutConstraints{
		Width:         width,
		Height:        height,
		IsNarrow:      isNarrow,
		IsWide:        isWide,
		IsTall:        isTall,
		IsShort:       isShort,
		ContentWidth:  contentWidth,
		ContentHeight: contentHeight,
		SidebarWidth:  sidebarWidth,
		MaxCardWidth:  maxCardWidth,
	}
}

// createResponsiveStyles generates size-adapted styling
func createResponsiveStyles(constraints LayoutConstraints) ResponsiveStyles {
	// Color scheme
	primaryColor := lipgloss.Color("63")
	secondaryColor := lipgloss.Color("212")
	errorColor := lipgloss.Color("196")
	mutedColor := lipgloss.Color("240")

	// Base styles adapted for size
	var titleStyle, subtitleStyle, descriptionStyle lipgloss.Style
	var cardStyle, inputStyle, menuStyle lipgloss.Style
	var headerStyle, contentStyle, sidebarStyle, footerStyle lipgloss.Style

	if constraints.IsNarrow {
		// Narrow terminal: minimal styling, compact layout
		titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(primaryColor).
			MarginBottom(1)

		subtitleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(secondaryColor)

		descriptionStyle = lipgloss.NewStyle().
			Foreground(mutedColor)

		cardStyle = lipgloss.NewStyle().
			Width(constraints.ContentWidth).
			Padding(1).
			Margin(0, 0, 1, 0).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(mutedColor)

		inputStyle = lipgloss.NewStyle().
			Width(constraints.ContentWidth-4).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(primaryColor).
			Padding(0, 1)

		menuStyle = lipgloss.NewStyle().
			Width(constraints.ContentWidth)

	} else if constraints.IsWide {
		// Wide terminal: full styling, spacious layout
		titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(primaryColor).
			MarginBottom(2).
			Underline(true).
			Align(lipgloss.Center)

		subtitleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(secondaryColor).
			MarginBottom(1).
			Align(lipgloss.Center)

		descriptionStyle = lipgloss.NewStyle().
			Foreground(mutedColor).
			MarginBottom(2).
			Align(lipgloss.Center)

		cardStyle = lipgloss.NewStyle().
			Width(constraints.MaxCardWidth).
			Padding(2, 3).
			Margin(1, 0, 2, 0).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(mutedColor)

		inputStyle = lipgloss.NewStyle().
			Width(constraints.MaxCardWidth-6).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(primaryColor).
			Padding(1, 2)

		menuStyle = lipgloss.NewStyle().
			Width(constraints.MaxCardWidth)

	} else {
		// Standard terminal: balanced styling
		titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(primaryColor).
			MarginBottom(1).
			Underline(true)

		subtitleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(secondaryColor).
			MarginBottom(1)

		descriptionStyle = lipgloss.NewStyle().
			Foreground(mutedColor).
			MarginBottom(1)

		cardStyle = lipgloss.NewStyle().
			Width(constraints.MaxCardWidth).
			Padding(1, 2).
			Margin(0, 0, 1, 0).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(mutedColor)

		inputStyle = lipgloss.NewStyle().
			Width(constraints.MaxCardWidth-4).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(primaryColor).
			Padding(0, 1)

		menuStyle = lipgloss.NewStyle().
			Width(constraints.MaxCardWidth)
	}

	// Layout containers
	headerStyle = lipgloss.NewStyle().
		Width(constraints.Width).
		Align(lipgloss.Center).
		Bold(true)

	contentStyle = lipgloss.NewStyle().
		Width(constraints.ContentWidth).
		Height(constraints.ContentHeight)

	sidebarStyle = lipgloss.NewStyle().
		Width(constraints.SidebarWidth).
		Height(constraints.ContentHeight).
		Border(lipgloss.RoundedBorder()).
		BorderForeground(mutedColor).
		Padding(1)

	footerStyle = lipgloss.NewStyle().
		Width(constraints.Width).
		Align(lipgloss.Center).
		Foreground(mutedColor)

	// Common styles
	helpStyle := lipgloss.NewStyle().
		Foreground(mutedColor).
		Italic(true).
		MarginTop(1)

	errorStyle := lipgloss.NewStyle().
		Foreground(errorColor).
		Bold(true)

	reportStyle := lipgloss.NewStyle().
		Width(constraints.MaxCardWidth).
		Padding(1, 2).
		Border(lipgloss.RoundedBorder()).
		BorderForeground(primaryColor)

	progressStyle := lipgloss.NewStyle().
		Width(constraints.ContentWidth - 10)

	// Adjust progress width for terminal size
	if progressStyle.GetWidth() > 120 {
		progressStyle = progressStyle.Width(120)
	}
	if progressStyle.GetWidth() < 20 {
		progressStyle = progressStyle.Width(20)
	}

	return ResponsiveStyles{
		Title:       titleStyle,
		Subtitle:    subtitleStyle,
		Description: descriptionStyle,
		Help:        helpStyle,
		Error:       errorStyle,
		Card:        cardStyle,
		Input:       inputStyle,
		Menu:        menuStyle,
		Report:      reportStyle,
		Progress:    progressStyle,
		Header:      headerStyle,
		Content:     contentStyle,
		Sidebar:     sidebarStyle,
		Footer:      footerStyle,
	}
}

// Update refreshes the layout for new terminal dimensions
func (rl *ResponsiveLayout) Update(width, height int) {
	rl.constraints = calculateConstraints(width, height)
	rl.styles = createResponsiveStyles(rl.constraints)
}

// GetConstraints returns the current layout constraints
func (rl *ResponsiveLayout) GetConstraints() LayoutConstraints {
	return rl.constraints
}

// GetStyles returns the current responsive styles
func (rl *ResponsiveLayout) GetStyles() ResponsiveStyles {
	return rl.styles
}

// WrapContent wraps content appropriately for the current terminal size
func (rl *ResponsiveLayout) WrapContent(content string) string {
	if rl.constraints.IsNarrow {
		// For narrow terminals, wrap more aggressively
		return lipgloss.NewStyle().
			Width(rl.constraints.ContentWidth).
			Render(content)
	} else if rl.constraints.IsWide {
		// For wide terminals, center content
		return lipgloss.NewStyle().
			Width(rl.constraints.MaxCardWidth).
			Align(lipgloss.Center).
			Render(content)
	}

	// Standard wrapping
	return lipgloss.NewStyle().
		Width(rl.constraints.MaxCardWidth).
		Render(content)
}

// FormatMenu formats menu items appropriately for terminal size
func (rl *ResponsiveLayout) FormatMenu(items []MenuItem) string {
	var content strings.Builder

	for i, item := range items {
		var style lipgloss.Style
		cursor := "  "

		if item.Selected {
			cursor = "▶ "
			style = lipgloss.NewStyle().
				Foreground(lipgloss.Color("212")).
				Bold(true)
		} else {
			style = lipgloss.NewStyle()
		}

		if !item.Enabled {
			style = style.Foreground(lipgloss.Color("240"))
			cursor = "  "
		}

		// Format based on terminal size
		if rl.constraints.IsNarrow {
			// Compact format for narrow terminals
			line := style.Render(cursor + item.Title)
			content.WriteString(line + "\n")
		} else {
			// Full format with description
			line := style.Render(cursor + item.Title)
			if item.Status != "" {
				line += " " + lipgloss.NewStyle().
					Foreground(lipgloss.Color("240")).
					Render(item.Status)
			}
			content.WriteString(line + "\n")

			if item.Description != "" {
				descLine := lipgloss.NewStyle().
					Foreground(lipgloss.Color("240")).
					Render("    " + item.Description)
				content.WriteString(descLine + "\n")
			}
		}

		if i < len(items)-1 {
			content.WriteString("\n")
		}
	}

	return content.String()
}

// FormatHelp formats help text appropriately for terminal size
func (rl *ResponsiveLayout) FormatHelp(shortcuts []HelpShortcut, tip string) string {
	var content strings.Builder

	if !rl.constraints.IsNarrow {
		content.WriteString("📋 Navigation:\n")
		for _, shortcut := range shortcuts {
			content.WriteString(fmt.Sprintf("  %-12s %s\n", shortcut.Keys, shortcut.Description))
		}
		if tip != "" {
			content.WriteString(fmt.Sprintf("\n💡 Tip: %s", tip))
		}
	} else {
		// Compact help for narrow terminals
		content.WriteString("Navigation: ")
		var shortKeys []string
		for _, shortcut := range shortcuts {
			shortKeys = append(shortKeys, shortcut.Keys)
		}
		content.WriteString(strings.Join(shortKeys, ", "))
		if tip != "" {
			content.WriteString(fmt.Sprintf("\nTip: %s", tip))
		}
	}

	return rl.styles.Help.Render(content.String())
}

// FormatError formats error messages appropriately for terminal size
func (rl *ResponsiveLayout) FormatError(err error) string {
	var content strings.Builder

	errorHeader := rl.styles.Error.Render("An Error Occurred")
	content.WriteString(errorHeader + "\n\n")

	// Wrap error message to fit terminal
	errorBody := lipgloss.NewStyle().
		Width(rl.constraints.MaxCardWidth).
		Render(err.Error())
	content.WriteString(errorBody + "\n\n")

	helpText := rl.styles.Help.Render("Press any key to return to the main menu.")
	content.WriteString(helpText)

	// Create bordered error box
	box := lipgloss.NewStyle().
		Border(lipgloss.NormalBorder(), true).
		BorderForeground(lipgloss.Color("240")).
		Padding(1, 2).
		Width(rl.constraints.MaxCardWidth).
		Render(content.String())

	// Center the error box if we have space
	if rl.constraints.IsWide {
		return lipgloss.Place(rl.constraints.Width, rl.constraints.Height,
			lipgloss.Center, lipgloss.Center, box)
	}

	return box
}

// Helper structures for formatting
type MenuItem struct {
	Title       string
	Description string
	Status      string
	Selected    bool
	Enabled     bool
}

type HelpShortcut struct {
	Keys        string
	Description string
}
