package theme

import (
	"fmt"

	"github.com/charmbracelet/lipgloss"
)

// ColorScheme defines the color palette for the TUI
type ColorScheme struct {
	// Primary colors
	Primary   lipgloss.Color
	Secondary lipgloss.Color
	Accent    lipgloss.Color

	// Status colors
	Success lipgloss.Color
	Warning lipgloss.Color
	Error   lipgloss.Color
	Info    lipgloss.Color

	// Neutral colors
	Background lipgloss.Color
	Foreground lipgloss.Color
	Muted      lipgloss.Color
	Border     lipgloss.Color

	// Interactive colors
	Selected lipgloss.Color
	Hover    lipgloss.Color
	Active   lipgloss.Color
	Disabled lipgloss.Color
}

// Theme defines the complete styling theme for the TUI
type Theme struct {
	Name          string
	Description   string
	Colors        ColorScheme
	Typography    TypographyStyles
	Components    ComponentStyles
	Layouts       LayoutStyles
	Animations    AnimationStyles
	Accessibility AccessibilityStyles
}

// TypographyStyles defines text styling
type TypographyStyles struct {
	// Headings
	H1 lipgloss.Style
	H2 lipgloss.Style
	H3 lipgloss.Style
	H4 lipgloss.Style

	// Body text
	Body      lipgloss.Style
	BodySmall lipgloss.Style
	BodyLarge lipgloss.Style
	Monospace lipgloss.Style

	// Special text
	Caption lipgloss.Style
	Label   lipgloss.Style
	Link    lipgloss.Style
	Code    lipgloss.Style
}

// ComponentStyles defines component-specific styling
type ComponentStyles struct {
	// Input components
	TextInput ComponentStyle
	Button    ComponentStyle
	Progress  ComponentStyle
	Spinner   ComponentStyle
	Checkbox  ComponentStyle

	// Display components
	Card  ComponentStyle
	Table ComponentStyle
	List  ComponentStyle
	Menu  ComponentStyle
	Badge ComponentStyle

	// Layout components
	Header       ComponentStyle
	Footer       ComponentStyle
	Sidebar      ComponentStyle
	Modal        ComponentStyle
	Notification ComponentStyle
}

// ComponentStyle defines styling for a single component
type ComponentStyle struct {
	Base     lipgloss.Style
	Focused  lipgloss.Style
	Hover    lipgloss.Style
	Active   lipgloss.Style
	Disabled lipgloss.Style
	Error    lipgloss.Style
}

// LayoutStyles defines layout-specific styling
type LayoutStyles struct {
	Container lipgloss.Style
	Section   lipgloss.Style
	Row       lipgloss.Style
	Column    lipgloss.Style
	Separator lipgloss.Style
	Spacer    lipgloss.Style
}

// AnimationStyles defines animation and transition styles
type AnimationStyles struct {
	FadeIn   lipgloss.Style
	FadeOut  lipgloss.Style
	SlideIn  lipgloss.Style
	SlideOut lipgloss.Style
	Pulse    lipgloss.Style
	Loading  lipgloss.Style
}

// AccessibilityStyles defines accessibility-focused styling
type AccessibilityStyles struct {
	HighContrast   bool
	LargeText      bool
	FocusIndicator lipgloss.Style
	ScreenReader   lipgloss.Style
}

// ThemeManager manages theme application and switching
type ThemeManager struct {
	themes        map[string]*Theme
	activeTheme   string
	fallbackTheme string
}

// NewThemeManager creates a new theme manager
func NewThemeManager() *ThemeManager {
	tm := &ThemeManager{
		themes:        make(map[string]*Theme),
		activeTheme:   "default",
		fallbackTheme: "default",
	}

	// Register built-in themes
	tm.RegisterTheme(createDefaultTheme())
	tm.RegisterTheme(createDarkTheme())
	tm.RegisterTheme(createLightTheme())
	tm.RegisterTheme(createHighContrastTheme())
	tm.RegisterTheme(createMinimalTheme())

	return tm
}

// RegisterTheme registers a new theme
func (tm *ThemeManager) RegisterTheme(theme *Theme) {
	tm.themes[theme.Name] = theme
}

// SetActiveTheme sets the active theme
func (tm *ThemeManager) SetActiveTheme(name string) error {
	if _, exists := tm.themes[name]; !exists {
		return fmt.Errorf("theme '%s' not found", name)
	}
	tm.activeTheme = name
	return nil
}

// GetActiveTheme returns the current active theme
func (tm *ThemeManager) GetActiveTheme() *Theme {
	if theme, exists := tm.themes[tm.activeTheme]; exists {
		return theme
	}
	// Fallback to default theme
	return tm.themes[tm.fallbackTheme]
}

// GetTheme returns a specific theme by name
func (tm *ThemeManager) GetTheme(name string) *Theme {
	if theme, exists := tm.themes[name]; exists {
		return theme
	}
	return nil
}

// ListThemes returns all available theme names
func (tm *ThemeManager) ListThemes() []string {
	names := make([]string, 0, len(tm.themes))
	for name := range tm.themes {
		names = append(names, name)
	}
	return names
}

// createDefaultTheme creates the default theme
func createDefaultTheme() *Theme {
	colors := ColorScheme{
		Primary:    lipgloss.Color("63"),  // Blue
		Secondary:  lipgloss.Color("212"), // Pink
		Accent:     lipgloss.Color("86"),  // Cyan
		Success:    lipgloss.Color("46"),  // Green
		Warning:    lipgloss.Color("202"), // Orange
		Error:      lipgloss.Color("196"), // Red
		Info:       lipgloss.Color("39"),  // Light Blue
		Background: lipgloss.Color("0"),   // Black
		Foreground: lipgloss.Color("255"), // White
		Muted:      lipgloss.Color("240"), // Gray
		Border:     lipgloss.Color("240"), // Gray
		Selected:   lipgloss.Color("212"), // Pink
		Hover:      lipgloss.Color("39"),  // Light Blue
		Active:     lipgloss.Color("46"),  // Green
		Disabled:   lipgloss.Color("240"), // Gray
	}

	typography := createTypographyStyles(colors)
	components := createComponentStyles(colors)
	layouts := createLayoutStyles(colors)
	animations := createAnimationStyles(colors)
	accessibility := createAccessibilityStyles(colors, false, false)

	return &Theme{
		Name:          "default",
		Description:   "Default theme with balanced colors and modern styling",
		Colors:        colors,
		Typography:    typography,
		Components:    components,
		Layouts:       layouts,
		Animations:    animations,
		Accessibility: accessibility,
	}
}

// createDarkTheme creates a dark theme
func createDarkTheme() *Theme {
	colors := ColorScheme{
		Primary:    lipgloss.Color("69"),  // Purple
		Secondary:  lipgloss.Color("213"), // Magenta
		Accent:     lipgloss.Color("51"),  // Cyan
		Success:    lipgloss.Color("120"), // Green
		Warning:    lipgloss.Color("214"), // Orange
		Error:      lipgloss.Color("203"), // Red
		Info:       lipgloss.Color("117"), // Light Blue
		Background: lipgloss.Color("16"),  // Dark
		Foreground: lipgloss.Color("252"), // Light Gray
		Muted:      lipgloss.Color("236"), // Dark Gray
		Border:     lipgloss.Color("236"), // Dark Gray
		Selected:   lipgloss.Color("213"), // Magenta
		Hover:      lipgloss.Color("117"), // Light Blue
		Active:     lipgloss.Color("120"), // Green
		Disabled:   lipgloss.Color("236"), // Dark Gray
	}

	typography := createTypographyStyles(colors)
	components := createComponentStyles(colors)
	layouts := createLayoutStyles(colors)
	animations := createAnimationStyles(colors)
	accessibility := createAccessibilityStyles(colors, false, false)

	return &Theme{
		Name:          "dark",
		Description:   "Dark theme optimized for low-light environments",
		Colors:        colors,
		Typography:    typography,
		Components:    components,
		Layouts:       layouts,
		Animations:    animations,
		Accessibility: accessibility,
	}
}

// createLightTheme creates a light theme
func createLightTheme() *Theme {
	colors := ColorScheme{
		Primary:    lipgloss.Color("21"),  // Blue
		Secondary:  lipgloss.Color("162"), // Pink
		Accent:     lipgloss.Color("30"),  // Cyan
		Success:    lipgloss.Color("22"),  // Green
		Warning:    lipgloss.Color("172"), // Orange
		Error:      lipgloss.Color("160"), // Red
		Info:       lipgloss.Color("31"),  // Light Blue
		Background: lipgloss.Color("255"), // White
		Foreground: lipgloss.Color("16"),  // Dark
		Muted:      lipgloss.Color("248"), // Light Gray
		Border:     lipgloss.Color("248"), // Light Gray
		Selected:   lipgloss.Color("162"), // Pink
		Hover:      lipgloss.Color("31"),  // Light Blue
		Active:     lipgloss.Color("22"),  // Green
		Disabled:   lipgloss.Color("248"), // Light Gray
	}

	typography := createTypographyStyles(colors)
	components := createComponentStyles(colors)
	layouts := createLayoutStyles(colors)
	animations := createAnimationStyles(colors)
	accessibility := createAccessibilityStyles(colors, false, false)

	return &Theme{
		Name:          "light",
		Description:   "Light theme for bright environments",
		Colors:        colors,
		Typography:    typography,
		Components:    components,
		Layouts:       layouts,
		Animations:    animations,
		Accessibility: accessibility,
	}
}

// createHighContrastTheme creates a high contrast theme for accessibility
func createHighContrastTheme() *Theme {
	colors := ColorScheme{
		Primary:    lipgloss.Color("15"),  // Bright White
		Secondary:  lipgloss.Color("226"), // Bright Yellow
		Accent:     lipgloss.Color("51"),  // Bright Cyan
		Success:    lipgloss.Color("46"),  // Bright Green
		Warning:    lipgloss.Color("226"), // Bright Yellow
		Error:      lipgloss.Color("196"), // Bright Red
		Info:       lipgloss.Color("51"),  // Bright Cyan
		Background: lipgloss.Color("0"),   // Black
		Foreground: lipgloss.Color("15"),  // Bright White
		Muted:      lipgloss.Color("7"),   // Light Gray
		Border:     lipgloss.Color("15"),  // Bright White
		Selected:   lipgloss.Color("226"), // Bright Yellow
		Hover:      lipgloss.Color("51"),  // Bright Cyan
		Active:     lipgloss.Color("46"),  // Bright Green
		Disabled:   lipgloss.Color("7"),   // Light Gray
	}

	typography := createTypographyStyles(colors)
	components := createComponentStyles(colors)
	layouts := createLayoutStyles(colors)
	animations := createAnimationStyles(colors)
	accessibility := createAccessibilityStyles(colors, true, false)

	return &Theme{
		Name:          "high-contrast",
		Description:   "High contrast theme for improved accessibility",
		Colors:        colors,
		Typography:    typography,
		Components:    components,
		Layouts:       layouts,
		Animations:    animations,
		Accessibility: accessibility,
	}
}

// createMinimalTheme creates a minimal theme
func createMinimalTheme() *Theme {
	colors := ColorScheme{
		Primary:    lipgloss.Color("255"), // White
		Secondary:  lipgloss.Color("248"), // Light Gray
		Accent:     lipgloss.Color("240"), // Gray
		Success:    lipgloss.Color("255"), // White
		Warning:    lipgloss.Color("255"), // White
		Error:      lipgloss.Color("255"), // White
		Info:       lipgloss.Color("255"), // White
		Background: lipgloss.Color("0"),   // Black
		Foreground: lipgloss.Color("255"), // White
		Muted:      lipgloss.Color("240"), // Gray
		Border:     lipgloss.Color("240"), // Gray
		Selected:   lipgloss.Color("248"), // Light Gray
		Hover:      lipgloss.Color("248"), // Light Gray
		Active:     lipgloss.Color("255"), // White
		Disabled:   lipgloss.Color("240"), // Gray
	}

	typography := createTypographyStyles(colors)
	components := createComponentStyles(colors)
	layouts := createLayoutStyles(colors)
	animations := createAnimationStyles(colors)
	accessibility := createAccessibilityStyles(colors, false, false)

	return &Theme{
		Name:          "minimal",
		Description:   "Minimal monochrome theme with clean aesthetics",
		Colors:        colors,
		Typography:    typography,
		Components:    components,
		Layouts:       layouts,
		Animations:    animations,
		Accessibility: accessibility,
	}
}

// createTypographyStyles creates typography styles from color scheme
func createTypographyStyles(colors ColorScheme) TypographyStyles {
	return TypographyStyles{
		H1: lipgloss.NewStyle().
			Bold(true).
			Foreground(colors.Primary).
			MarginBottom(2).
			Underline(true),
		H2: lipgloss.NewStyle().
			Bold(true).
			Foreground(colors.Primary).
			MarginBottom(1).
			Underline(true),
		H3: lipgloss.NewStyle().
			Bold(true).
			Foreground(colors.Secondary).
			MarginBottom(1),
		H4: lipgloss.NewStyle().
			Bold(true).
			Foreground(colors.Secondary),
		Body: lipgloss.NewStyle().
			Foreground(colors.Foreground),
		BodySmall: lipgloss.NewStyle().
			Foreground(colors.Muted),
		BodyLarge: lipgloss.NewStyle().
			Foreground(colors.Foreground),
		Monospace: lipgloss.NewStyle().
			Foreground(colors.Foreground),
		Caption: lipgloss.NewStyle().
			Foreground(colors.Muted).
			Italic(true),
		Label: lipgloss.NewStyle().
			Foreground(colors.Secondary).
			Bold(true),
		Link: lipgloss.NewStyle().
			Foreground(colors.Accent).
			Underline(true),
		Code: lipgloss.NewStyle().
			Foreground(colors.Accent).
			Background(colors.Border).
			Padding(0, 1),
	}
}

// createComponentStyles creates component styles from color scheme
func createComponentStyles(colors ColorScheme) ComponentStyles {
	return ComponentStyles{
		TextInput: ComponentStyle{
			Base: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colors.Border).
				Padding(0, 1).
				Foreground(colors.Foreground),
			Focused: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colors.Primary).
				Padding(0, 1).
				Foreground(colors.Foreground),
			Error: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colors.Error).
				Padding(0, 1).
				Foreground(colors.Error),
		},
		Button: ComponentStyle{
			Base: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colors.Border).
				Padding(0, 2).
				Foreground(colors.Foreground),
			Focused: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colors.Primary).
				Padding(0, 2).
				Foreground(colors.Primary).
				Bold(true),
			Hover: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colors.Hover).
				Padding(0, 2).
				Foreground(colors.Hover),
			Active: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colors.Active).
				Background(colors.Active).
				Padding(0, 2).
				Foreground(colors.Background),
			Disabled: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colors.Disabled).
				Padding(0, 2).
				Foreground(colors.Disabled),
		},
		Card: ComponentStyle{
			Base: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colors.Border).
				Padding(1, 2).
				Margin(0, 0, 1, 0),
			Focused: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colors.Primary).
				Padding(1, 2).
				Margin(0, 0, 1, 0),
		},
		Menu: ComponentStyle{
			Base: lipgloss.NewStyle().
				Foreground(colors.Foreground),
			Focused: lipgloss.NewStyle().
				Foreground(colors.Selected).
				Bold(true),
		},
		Badge: ComponentStyle{
			Base: lipgloss.NewStyle().
				Background(colors.Muted).
				Foreground(colors.Background).
				Padding(0, 1).
				Border(lipgloss.RoundedBorder()),
		},
		Header: ComponentStyle{
			Base: lipgloss.NewStyle().
				Bold(true).
				Foreground(colors.Primary).
				Align(lipgloss.Center).
				MarginBottom(1),
		},
		Footer: ComponentStyle{
			Base: lipgloss.NewStyle().
				Foreground(colors.Muted).
				Align(lipgloss.Center).
				MarginTop(1),
		},
		Sidebar: ComponentStyle{
			Base: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colors.Border).
				Padding(1),
		},
		Modal: ComponentStyle{
			Base: lipgloss.NewStyle().
				Border(lipgloss.ThickBorder()).
				BorderForeground(colors.Primary).
				Background(colors.Background).
				Padding(2),
		},
		Notification: ComponentStyle{
			Base: lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colors.Info).
				Background(colors.Background).
				Padding(1, 2),
		},
	}
}

// createLayoutStyles creates layout styles from color scheme
func createLayoutStyles(colors ColorScheme) LayoutStyles {
	return LayoutStyles{
		Container: lipgloss.NewStyle().
			Padding(1),
		Section: lipgloss.NewStyle().
			Margin(1, 0),
		Row:    lipgloss.NewStyle(),
		Column: lipgloss.NewStyle(),
		Separator: lipgloss.NewStyle().
			Foreground(colors.Border).
			MarginTop(1).
			MarginBottom(1),
		Spacer: lipgloss.NewStyle().
			Height(1),
	}
}

// createAnimationStyles creates animation styles from color scheme
func createAnimationStyles(colors ColorScheme) AnimationStyles {
	return AnimationStyles{
		FadeIn: lipgloss.NewStyle().
			Foreground(colors.Foreground),
		FadeOut: lipgloss.NewStyle().
			Foreground(colors.Muted),
		Pulse: lipgloss.NewStyle().
			Foreground(colors.Primary),
		Loading: lipgloss.NewStyle().
			Foreground(colors.Accent),
	}
}

// createAccessibilityStyles creates accessibility styles
func createAccessibilityStyles(colors ColorScheme, highContrast, largeText bool) AccessibilityStyles {
	focusIndicator := lipgloss.NewStyle().
		Border(lipgloss.DoubleBorder()).
		BorderForeground(colors.Primary)

	if highContrast {
		focusIndicator = focusIndicator.
			Border(lipgloss.ThickBorder()).
			BorderForeground(colors.Primary)
	}

	return AccessibilityStyles{
		HighContrast:   highContrast,
		LargeText:      largeText,
		FocusIndicator: focusIndicator,
		ScreenReader: lipgloss.NewStyle().
			Foreground(colors.Foreground),
	}
}

// ApplyTheme applies theme styles to components
func (t *Theme) ApplyTheme() {
	// This would be called to apply the theme globally
	// Implementation depends on how the TUI components are structured
}

// GetComponentStyle returns the appropriate component style
func (t *Theme) GetComponentStyle(component string, state string) lipgloss.Style {
	var componentStyle ComponentStyle

	switch component {
	case "textinput":
		componentStyle = t.Components.TextInput
	case "button":
		componentStyle = t.Components.Button
	case "card":
		componentStyle = t.Components.Card
	case "menu":
		componentStyle = t.Components.Menu
	case "badge":
		componentStyle = t.Components.Badge
	case "header":
		componentStyle = t.Components.Header
	case "footer":
		componentStyle = t.Components.Footer
	case "sidebar":
		componentStyle = t.Components.Sidebar
	case "modal":
		componentStyle = t.Components.Modal
	case "notification":
		componentStyle = t.Components.Notification
	default:
		return lipgloss.NewStyle()
	}

	switch state {
	case "focused", "focus":
		return componentStyle.Focused
	case "hover":
		return componentStyle.Hover
	case "active":
		return componentStyle.Active
	case "disabled":
		return componentStyle.Disabled
	case "error":
		return componentStyle.Error
	default:
		return componentStyle.Base
	}
}

// GetTypographyStyle returns the appropriate typography style
func (t *Theme) GetTypographyStyle(element string) lipgloss.Style {
	switch element {
	case "h1":
		return t.Typography.H1
	case "h2":
		return t.Typography.H2
	case "h3":
		return t.Typography.H3
	case "h4":
		return t.Typography.H4
	case "body":
		return t.Typography.Body
	case "body-small":
		return t.Typography.BodySmall
	case "body-large":
		return t.Typography.BodyLarge
	case "monospace":
		return t.Typography.Monospace
	case "caption":
		return t.Typography.Caption
	case "label":
		return t.Typography.Label
	case "link":
		return t.Typography.Link
	case "code":
		return t.Typography.Code
	default:
		return t.Typography.Body
	}
}

// GetLayoutStyle returns the appropriate layout style
func (t *Theme) GetLayoutStyle(element string) lipgloss.Style {
	switch element {
	case "container":
		return t.Layouts.Container
	case "section":
		return t.Layouts.Section
	case "row":
		return t.Layouts.Row
	case "column":
		return t.Layouts.Column
	case "separator":
		return t.Layouts.Separator
	case "spacer":
		return t.Layouts.Spacer
	default:
		return lipgloss.NewStyle()
	}
}

// IsAccessibilityEnabled returns whether accessibility features are enabled
func (t *Theme) IsAccessibilityEnabled() bool {
	return t.Accessibility.HighContrast || t.Accessibility.LargeText
}

// GetAccessibilityFocusStyle returns the accessibility focus indicator style
func (t *Theme) GetAccessibilityFocusStyle() lipgloss.Style {
	return t.Accessibility.FocusIndicator
}
