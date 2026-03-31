// Package tui runs the interactive terminal experience for data-refinery.
package tui

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/charmbracelet/bubbles/progress"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/benjaminwestern/data-refinery/internal/analyser"
	"github.com/benjaminwestern/data-refinery/internal/backup"
	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/deletion"
	"github.com/benjaminwestern/data-refinery/internal/errors"
	"github.com/benjaminwestern/data-refinery/internal/memory"
	"github.com/benjaminwestern/data-refinery/internal/output"
	"github.com/benjaminwestern/data-refinery/internal/report"
	"github.com/benjaminwestern/data-refinery/internal/safety"
	"github.com/benjaminwestern/data-refinery/internal/source"
	"github.com/benjaminwestern/data-refinery/internal/state"
)

// View states - simplified and grouped logically.
const (
	viewMenu int = iota
	viewOptions
	viewHelp
	viewInputPath
	viewInputKey
	viewInputLogPath
	viewProcessing
	viewCancelling
	viewReport
	viewPurgeSelection
	viewPurging

	// Advanced feature views.
	viewAdvancedMenu
	viewSchemaConfig
	viewSchemaResult
	viewSearchConfig
	viewSearchTargetEdit
	viewHashingConfig
	viewDeletionConfig
	viewDeletionRuleEdit
	viewAdvancedOutput
)

const (
	keyDown             = "down"
	keyEnter            = "enter"
	menuCursorGlyph     = "▶ "
	hashModeSelective   = "selective"
	hashModeExcludeKeys = "exclude_keys"
)

// Enhanced styling with consistent theme.
var (
	// Enhanced color scheme.
	primaryColor   = lipgloss.Color("63")
	secondaryColor = lipgloss.Color("212")
	successColor   = lipgloss.Color("46")
	warningColor   = lipgloss.Color("202")
	errorColor     = lipgloss.Color("196")
	mutedColor     = lipgloss.Color("240")

	// Core styles with improved consistency.
	spinnerStyle = lipgloss.NewStyle().Foreground(primaryColor)
	statusStyle  = lipgloss.NewStyle().MarginLeft(1).Foreground(primaryColor).Bold(true)

	helpStyle = lipgloss.NewStyle().
			Foreground(mutedColor).
			Italic(true).
			Margin(1, 0)

	timingStyle = lipgloss.NewStyle().Foreground(mutedColor)

	errorStyle = lipgloss.NewStyle().
			Foreground(errorColor).
			Bold(true)

	headerStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(primaryColor).
			MarginBottom(1).
			Underline(true)

	menuCursorStyle = lipgloss.NewStyle().
			Foreground(secondaryColor).
			Bold(true)

	selectionStyle = lipgloss.NewStyle().
			Foreground(warningColor).
			Bold(true)

	reportStyle = lipgloss.NewStyle().
			Padding(0, 1).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(primaryColor)

	// New enhanced styles for better UX.
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

	inputStyle = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(primaryColor).
			Padding(0, 1)

	cardStyle = lipgloss.NewStyle().
			Padding(1, 2).
			Margin(0, 0, 1, 0).
			Border(lipgloss.RoundedBorder()).
			BorderForeground(mutedColor)

	successStyle = lipgloss.NewStyle().
			Foreground(successColor).
			Bold(true)

	warningStyle = lipgloss.NewStyle().
			Foreground(warningColor).
			Bold(true)
)

type (
	sourcesFoundMsg    struct{ sources []source.InputSource }
	progressUpdateMsg  struct{}
	allWorkCompleteMsg struct {
		report            *report.AnalysisReport
		savedFilenameBase string
	}
)

type purgeResultMsg struct {
	filesModified  int
	recordsDeleted int
	err            error
}
type errMsg struct{ err error }

type model struct {
	ctx context.Context

	jobCtx          context.Context
	jobCancel       context.CancelFunc
	wasCancelled    bool
	processing      bool
	analyser        *analyser.Analyser
	originalSources []source.InputSource
	isValidationRun bool

	// State management
	stateManager  *state.Manager
	memoryManager *memory.Manager

	viewState       int
	quitting        bool
	err             error
	status          string
	wantsToRestart  bool
	wantsToStartNew bool
	gcsAvailable    bool
	width           int
	height          int

	pathInput    textinput.Model
	keyInput     textinput.Model
	logPathInput textinput.Model
	spinner      spinner.Model
	progress     progress.Model

	startTime        time.Time
	totalElapsedTime time.Duration
	eta              time.Duration
	finalReport      *report.AnalysisReport
	savedFilename    string

	path                 string
	key                  string
	xmlRecordPath        string
	workers              int
	logPath              string
	approvedOutputRoot   string
	checkKey             bool
	checkRow             bool
	showFolderBreakdown  bool
	outputTxt            bool
	outputJSON           bool
	purgeIDs             bool
	purgeRows            bool
	loadedConfigPath     string
	configImplicit       bool
	allowImplicitConfig  bool
	unsafeMutationBypass bool

	menuCursor    int
	optionsCursor int

	purgeIDKeys          []string
	purgeRowHashes       []string
	purgeCursor          int
	purgeSelectionCursor int
	recordsToDelete      map[string]map[int]bool
	purgeStats           purgeResultMsg

	// Advanced feature state
	advancedEnabled      bool
	advancedMenuCursor   int
	schemaConfigCursor   int
	searchConfigCursor   int
	hashingConfigCursor  int
	deletionConfigCursor int
	outputConfigCursor   int

	// Advanced configuration
	searchTargets   []config.SearchTarget
	hashingStrategy config.HashingStrategy
	deletionRules   []config.DeletionRule
	schemaDiscovery config.SchemaDiscoveryConfig

	// Edit state for advanced features
	editingSearchTarget *config.SearchTarget
	editingDeletionRule *config.DeletionRule
	editingTargetCursor int
	editingRuleCursor   int

	// Text inputs for advanced features
	searchNameInput     textinput.Model
	searchPathInput     textinput.Model
	searchValuesInput   textinput.Model
	ruleOutputPathInput textinput.Model
	hashKeysInput       textinput.Model
}

func testGCSClient() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Printf("GCS client pre-flight check failed: %v. GCS functionality will be disabled.", err)
		return false
	}
	safety.Close(client, "GCS preflight client")
	return true
}

// Run starts the interactive TUI and returns the final configuration and restart flags.
func Run(cfg *config.Config) (*config.Config, bool, bool, error) {
	cfg.GCSAvailable = testGCSClient()
	ctx := context.Background()
	m := initModel(ctx, cfg)

	p := tea.NewProgram(m, tea.WithAltScreen())
	finalModel, err := p.Run()
	if err != nil {
		return nil, false, false, fmt.Errorf("error running TUI: %w", err)
	}

	fm, ok := finalModel.(model)
	if !ok {
		return nil, false, false, fmt.Errorf("could not cast final model")
	}

	return fm.buildConfig(), fm.wantsToRestart, fm.wantsToStartNew, nil
}

func initModel(ctx context.Context, cfg *config.Config) model {
	pathInput := textinput.New()
	if cfg.GCSAvailable {
		pathInput.Placeholder = "/path/a,/path/b,gs://bucket/c"
	} else {
		pathInput.Placeholder = "/path/a,/path/b (GCS unavailable)"
	}
	pathInput.Focus()
	pathInput.SetValue(cfg.Path)

	keyInput := textinput.New()
	keyInput.Placeholder = "id"
	keyInput.SetValue(cfg.Key)

	logPathInput := textinput.New()
	logPathInput.SetValue(cfg.LogPath)

	// Advanced feature text inputs
	searchNameInput := textinput.New()
	searchNameInput.Placeholder = "target_name"

	searchPathInput := textinput.New()
	searchPathInput.Placeholder = "id or items[*].id"

	searchValuesInput := textinput.New()
	searchValuesInput.Placeholder = "value1,value2,value3"

	ruleOutputPathInput := textinput.New()
	ruleOutputPathInput.Placeholder = "output.jsonl"

	hashKeysInput := textinput.New()
	hashKeysInput.Placeholder = "timestamp,version,updated_at"

	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = spinnerStyle
	p := progress.New(progress.WithDefaultGradient())

	// Initialize state manager
	stateManager, err := state.NewStateManager("./state", "tui-session")
	if err != nil {
		log.Printf("Warning: Failed to initialize state manager: %v", err)
	}

	// Initialize memory manager
	memoryManager := memory.NewMemoryManager(1024) // 1GB default
	memoryManager.Start()                          // Start memory monitoring

	m := model{
		ctx:                 ctx,
		pathInput:           pathInput,
		keyInput:            keyInput,
		logPathInput:        logPathInput,
		searchNameInput:     searchNameInput,
		searchPathInput:     searchPathInput,
		searchValuesInput:   searchValuesInput,
		ruleOutputPathInput: ruleOutputPathInput,
		hashKeysInput:       hashKeysInput,
		spinner:             s,
		progress:            p,
		recordsToDelete:     make(map[string]map[int]bool),
		viewState:           viewMenu,
		gcsAvailable:        cfg.GCSAvailable,
		stateManager:        stateManager,
		memoryManager:       memoryManager,

		path:                 cfg.Path,
		key:                  cfg.Key,
		xmlRecordPath:        cfg.XMLRecordPath,
		workers:              cfg.Workers,
		logPath:              cfg.LogPath,
		approvedOutputRoot:   cfg.ApprovedOutputRoot,
		checkKey:             cfg.CheckKey,
		checkRow:             cfg.CheckRow,
		showFolderBreakdown:  cfg.ShowFolderBreakdown,
		outputTxt:            cfg.EnableTxtOutput,
		outputJSON:           cfg.EnableJSONOutput,
		purgeIDs:             cfg.PurgeIDs,
		purgeRows:            cfg.PurgeRows,
		loadedConfigPath:     cfg.LoadedConfigPath,
		configImplicit:       cfg.ConfigLoadedImplicitly,
		allowImplicitConfig:  cfg.AllowImplicitMutationConfig,
		unsafeMutationBypass: cfg.UnsafeMutationBypass,

		// Initialize advanced features
		advancedEnabled: cfg.Advanced != nil,
		hashingStrategy: config.HashingStrategy{Mode: "full_row"},
		schemaDiscovery: config.SchemaDiscoveryConfig{
			Enabled:       false,
			SamplePercent: 0.1,
			MaxDepth:      10,
			MaxSamples:    100000,
			OutputFormats: []string{"json"},
			GroupByFolder: false,
		},
	}

	// Load advanced config if present
	if cfg.Advanced != nil {
		m.searchTargets = cfg.Advanced.SearchTargets
		m.hashingStrategy = cfg.Advanced.HashingStrategy
		m.deletionRules = cfg.Advanced.DeletionRules
		m.schemaDiscovery = cfg.Advanced.SchemaDiscovery
	}

	if m.path != "" {
		m.viewState = viewProcessing
	}

	return m
}

func (m model) Init() tea.Cmd {
	if m.viewState == viewProcessing {
		paths := strings.Split(m.path, ",")
		for _, p := range paths {
			if strings.HasPrefix(strings.TrimSpace(p), "gs://") && !m.gcsAvailable {
				return func() tea.Msg {
					return errMsg{fmt.Errorf("cannot process GCS path: GCS credentials not available")}
				}
			}
		}
		return discoverAllSourcesCmd(m.ctx, paths)
	}
	return textinput.Blink
}

func (m *model) buildConfig() *config.Config {
	cfg := &config.Config{
		Path:                        m.path,
		Key:                         m.key,
		XMLRecordPath:               m.xmlRecordPath,
		Workers:                     m.workers,
		LogPath:                     m.logPath,
		ApprovedOutputRoot:          m.approvedOutputRoot,
		CheckKey:                    m.checkKey,
		CheckRow:                    m.checkRow,
		ShowFolderBreakdown:         m.showFolderBreakdown,
		EnableTxtOutput:             m.outputTxt,
		EnableJSONOutput:            m.outputJSON,
		PurgeIDs:                    m.purgeIDs,
		PurgeRows:                   m.purgeRows,
		LoadedConfigPath:            m.loadedConfigPath,
		ConfigLoadedImplicitly:      m.configImplicit,
		AllowImplicitMutationConfig: m.allowImplicitConfig,
		UnsafeMutationBypass:        m.unsafeMutationBypass,
	}

	// Add advanced config if enabled
	if m.advancedEnabled {
		cfg.Advanced = &config.AdvancedConfig{
			SearchTargets:   m.searchTargets,
			HashingStrategy: m.hashingStrategy,
			DeletionRules:   m.deletionRules,
			SchemaDiscovery: m.schemaDiscovery,
		}
	}

	return cfg
}

func saveConfigCmd(cfg *config.Config) tea.Cmd {
	return func() tea.Msg {
		if err := cfg.Save(); err != nil {
			log.Printf("Failed to save config: %v", err)
		}
		return nil
	}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	if m.quitting {
		return m, tea.Quit
	}

	switch msg := msg.(type) {
	case tea.KeyMsg:
		if updatedModel, keyCmd, handled := handleGlobalKey(m, msg); handled {
			return updatedModel, keyCmd
		}
		if msg.Type == tea.KeyEsc {
			switch m.viewState {
			case viewHelp, viewOptions, viewInputPath, viewReport:
				m.viewState = viewMenu
				return m, nil
			case viewAdvancedMenu, viewSchemaConfig, viewSearchConfig, viewHashingConfig, viewDeletionConfig, viewAdvancedOutput:
				m.viewState = viewAdvancedMenu
				return m, nil
			case viewSearchTargetEdit:
				m.viewState = viewSearchConfig
				m.searchNameInput.Blur()
				m.searchPathInput.Blur()
				m.searchValuesInput.Blur()
				m.editingSearchTarget = nil
				return m, nil
			case viewDeletionRuleEdit:
				m.viewState = viewDeletionConfig
				m.ruleOutputPathInput.Blur()
				m.editingDeletionRule = nil
				return m, nil
			case viewSchemaResult:
				m.viewState = viewSchemaConfig
				return m, nil
			case viewInputKey:
				m.viewState = viewInputPath
				m.keyInput.Blur()
				m.pathInput.Focus()
				return m, textinput.Blink
			case viewInputLogPath:
				m.viewState = viewOptions
				m.logPathInput.Blur()
				return m, nil
			case viewPurgeSelection:
				m.viewState = viewReport
				m.purgeCursor = 0
				m.purgeSelectionCursor = 0
				m.recordsToDelete = make(map[string]map[int]bool)
				m.purgeIDKeys = nil
				m.purgeRowHashes = nil
				return m, nil
			}
		}
	}

	switch m.viewState {
	case viewMenu:
		return updateMenu(m, msg)
	case viewOptions:
		return updateOptions(m, msg)
	case viewHelp:
		if _, ok := msg.(tea.KeyMsg); ok {
			m.viewState = viewMenu
		}
		return m, nil
	case viewInputPath:
		return updateInputPath(m, msg)
	case viewInputKey:
		return updateInputKey(m, msg)
	case viewInputLogPath:
		return updateInputLogPath(m, msg)
	case viewReport:
		return updateReport(m, msg)
	case viewPurgeSelection:
		return updatePurgeSelection(m, msg)
	case viewAdvancedMenu:
		return updateAdvancedMenu(m, msg)
	case viewSchemaConfig:
		return updateSchemaConfig(m, msg)
	case viewSchemaResult:
		return updateSchemaResult(m, msg)
	case viewSearchConfig:
		return updateSearchConfig(m, msg)
	case viewSearchTargetEdit:
		return updateSearchTargetEdit(m, msg)
	case viewHashingConfig:
		return updateHashingConfig(m, msg)
	case viewDeletionConfig:
		return updateDeletionConfig(m, msg)
	case viewDeletionRuleEdit:
		return updateDeletionRuleEdit(m, msg)
	case viewAdvancedOutput:
		return updateAdvancedOutput(m, msg)
	}

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.progress.Width = msg.Width - 10
		if m.progress.Width > 120 {
			m.progress.Width = 120
		}
		return m, nil
	case sourcesFoundMsg:
		m.originalSources = msg.sources
		m.processing = true
		m.totalElapsedTime = 0
		m.startTime = time.Now()
		cfg := m.buildConfig()
		cfg.IsValidationRun = m.isValidationRun
		analyser, err := analyser.New(cfg)
		if err != nil {
			return m, func() tea.Msg { return errMsg{err} }
		}
		m.analyser = analyser
		m.jobCtx, m.jobCancel = safety.ManagedContext(m.ctx)

		if m.isValidationRun {
			m.status = fmt.Sprintf("Found %d files. Validating key '%s'...", len(m.originalSources), m.key)
		} else {
			m.status = fmt.Sprintf("Found %d files. Analysing...", len(m.originalSources))
		}

		return m, tea.Batch(
			startAnalysisCmd(m.jobCtx, m.analyser, m.originalSources, m.buildConfig()),
			m.spinner.Tick,
			pollProgressCmd(&m),
		)
	case progressUpdateMsg:
		return updateProgress(m)
	case progress.FrameMsg:
		progressModel, cmd := m.progress.Update(msg)
		if newModel, ok := progressModel.(progress.Model); ok {
			m.progress = newModel
		}
		return m, cmd
	case spinner.TickMsg:
		if m.viewState == viewProcessing || m.viewState == viewCancelling || m.viewState == viewPurging {
			m.spinner, cmd = m.spinner.Update(msg)
			return m, cmd
		}
		return m, nil
	case allWorkCompleteMsg:
		m.progress.SetPercent(1.0)
		if !m.startTime.IsZero() {
			m.totalElapsedTime += time.Since(m.startTime)
			m.startTime = time.Time{}
		}
		msg.report.Summary.TotalElapsedTime = m.totalElapsedTime.Round(time.Second).String()
		m.finalReport = msg.report
		m.savedFilename = msg.savedFilenameBase
		m.viewState = viewReport
		return m, nil
	case purgeResultMsg:
		m.purgeStats = msg
		m.viewState = viewReport
		return m, nil
	case errMsg:
		m.err = msg.err
		if m.viewState == viewProcessing {
			m.viewState = viewMenu
		}
		return m, nil
	}
	return m, nil
}

func (m model) View() string {
	if m.quitting {
		return "Exiting...\n"
	}

	if m.err != nil {
		maxContentWidth := 80
		if contentWidth := m.width - 8; m.width > 0 && contentWidth < maxContentWidth {
			maxContentWidth = contentWidth
		}

		errorHeader := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("196")).Render("An Error Occurred")
		errorBodyStyle := lipgloss.NewStyle().Width(maxContentWidth)
		errorBody := errorBodyStyle.Render(fmt.Sprintf("%v", m.err))
		helpText := helpStyle.Render("\nPress any key to return to the main menu.")

		content := lipgloss.JoinVertical(lipgloss.Left, errorHeader, "\n", errorBody, "\n", helpText)
		box := lipgloss.NewStyle().
			Border(lipgloss.NormalBorder(), true).
			BorderForeground(lipgloss.Color("240")).
			Padding(1, 2).
			Render(content)

		return lipgloss.Place(m.width, m.height, lipgloss.Center, lipgloss.Center, box)
	}

	switch m.viewState {
	case viewMenu:
		return renderMenu(&m)
	case viewOptions:
		return renderOptions(&m)
	case viewHelp:
		return renderHelp(&m)
	case viewInputPath:
		return renderInputPath(&m)
	case viewInputKey:
		return renderInputKey(&m)
	case viewInputLogPath:
		return renderInputLogPath(&m)
	case viewProcessing, viewCancelling:
		return renderProcessing(&m)
	case viewReport:
		return renderReport(&m)
	case viewPurgeSelection:
		return renderPurgeSelection(&m)
	case viewPurging:
		return fmt.Sprintf("\n%s %s\n", m.spinner.View(), m.status)
	case viewAdvancedMenu:
		return renderAdvancedMenu(&m)
	case viewSchemaConfig:
		return renderSchemaConfig(&m)
	case viewSchemaResult:
		return renderSchemaResult()
	case viewSearchConfig:
		return renderSearchConfig(&m)
	case viewSearchTargetEdit:
		return renderSearchTargetEdit(&m)
	case viewHashingConfig:
		return renderHashingConfig(&m)
	case viewDeletionConfig:
		return renderDeletionConfig(&m)
	case viewDeletionRuleEdit:
		return renderDeletionRuleEdit(&m)
	case viewAdvancedOutput:
		return renderAdvancedOutput(&m)
	}
	return ""
}

func discoverAllSourcesCmd(ctx context.Context, paths []string) tea.Cmd {
	return func() tea.Msg {
		sources, err := source.DiscoverAllWithOptions(ctx, paths, source.DefaultAnalysisDiscoveryOptions())
		if err != nil {
			if ctx.Err() == context.Canceled {
				return nil
			}
			return errMsg{err}
		}
		return sourcesFoundMsg{sources: sources}
	}
}

func guardedAnalysisSafetyApplies(cfg *config.Config) bool {
	if cfg == nil {
		return false
	}

	if cfg.PurgeIDs || cfg.PurgeRows {
		return true
	}

	if cfg.Advanced == nil {
		return false
	}

	for _, rule := range cfg.Advanced.DeletionRules {
		if strings.TrimSpace(rule.OutputPath) != "" {
			return true
		}
	}

	return false
}

func validateGuardedAnalysisSafety(cfg *config.Config) (string, []string, []string, error) {
	if !guardedAnalysisSafetyApplies(cfg) {
		return "", nil, nil, nil
	}

	if cfg.ConfigLoadedImplicitly && !cfg.AllowImplicitMutationConfig && !cfg.UnsafeMutationBypass {
		return "", nil, nil, fmt.Errorf(
			"analysis is using implicitly discovered app config %s; rerun with --app-config, --allow-implicit-config, or --yes-i-know-what-im-doing",
			cfg.LoadedConfigPath,
		)
	}

	localTargets := []string{cfg.LogPath}
	if cfg.PurgeIDs || cfg.PurgeRows {
		localTargets = append(localTargets, "deleted_records")
	}

	var remoteTargets []string
	if cfg.Advanced != nil {
		for _, rule := range cfg.Advanced.DeletionRules {
			if rule.OutputPath == "" {
				continue
			}
			if strings.HasPrefix(rule.OutputPath, "gs://") {
				remoteTargets = append(remoteTargets, rule.OutputPath)
				continue
			}
			localTargets = append(localTargets, rule.OutputPath)
		}
	}

	resolvedRoot, err := config.ResolveApprovedOutputRoot(cfg.ApprovedOutputRoot)
	if err != nil {
		return "", nil, nil, fmt.Errorf("resolve approved output root: %w", err)
	}

	resolvedLocalTargets := make([]string, 0, len(localTargets))
	for _, target := range localTargets {
		if strings.TrimSpace(target) == "" {
			continue
		}
		absTarget, err := filepath.Abs(target)
		if err != nil {
			return "", nil, nil, fmt.Errorf("resolve guarded analysis write target %q: %w", target, err)
		}
		resolvedLocalTargets = append(resolvedLocalTargets, absTarget)
	}

	if cfg.UnsafeMutationBypass {
		return resolvedRoot, dedupeStrings(resolvedLocalTargets), dedupeStrings(remoteTargets), nil
	}

	validatedTargets, err := config.ValidateLocalWriteTargets(resolvedRoot, localTargets)
	if err != nil {
		return "", nil, nil, fmt.Errorf("%w. Use --approved-output-root or --yes-i-know-what-im-doing", err)
	}

	return resolvedRoot, validatedTargets, dedupeStrings(remoteTargets), nil
}

func dedupeStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	deduped := make([]string, 0, len(values))

	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		deduped = append(deduped, trimmed)
	}

	return deduped
}

func logGuardedAnalysisSafety(cfg *config.Config, sources []source.InputSource, approvedRoot string, localTargets, remoteTargets []string) {
	if !guardedAnalysisSafetyApplies(cfg) {
		return
	}

	log.Printf("analysis safety preflight")
	log.Printf("  app config: %s", cfg.ConfigSourceSummary())
	log.Printf("  approved local output root: %s", approvedRoot)
	if cfg.UnsafeMutationBypass {
		log.Printf("  safety bypass: enabled via --yes-i-know-what-im-doing")
	}
	for _, target := range localTargets {
		log.Printf("  local write target: %s", target)
	}
	for _, target := range remoteTargets {
		log.Printf("  remote write target: %s", target)
	}
	for _, src := range sources {
		if strings.HasPrefix(src.Path(), "gs://") {
			log.Printf("  remote mutation target: %s", src.Path())
			continue
		}
		log.Printf("  local mutation target: %s", src.Path())
	}
}

func startAnalysisCmd(ctx context.Context, a *analyser.Analyser, sources []source.InputSource, cfg *config.Config) tea.Cmd {
	return func() tea.Msg {
		approvedRoot, localTargets, remoteTargets, err := validateGuardedAnalysisSafety(cfg)
		if err != nil {
			return errMsg{err}
		}
		logGuardedAnalysisSafety(cfg, sources, approvedRoot, localTargets, remoteTargets)

		finalReport := a.Run(ctx, sources)
		if ctx.Err() == context.Canceled {
			if a.ProcessedFiles.Load() == 0 {
				return nil
			}
		}
		filenameBase := report.SaveAndLog(finalReport, cfg.LogPath, cfg.EnableTxtOutput, cfg.EnableJSONOutput, cfg.CheckKey, cfg.CheckRow, cfg.ShowFolderBreakdown)
		if err := output.WriteAdvancedArtifacts(cfg.LogPath, cfg, finalReport); err != nil {
			log.Printf("Failed to write advanced output files: %v", err)
		}
		return allWorkCompleteMsg{report: finalReport, savedFilenameBase: filenameBase}
	}
}

func pollProgressCmd(m *model) tea.Cmd {
	return tea.Tick(time.Millisecond*100, func(_ time.Time) tea.Msg {
		if m.analyser == nil {
			return progressUpdateMsg{}
		}
		if m.viewState != viewProcessing {
			return nil
		}
		return progressUpdateMsg{}
	})
}

func performPurgeCmd(cfg *config.Config, recordsToDelete map[string]map[int]bool) tea.Cmd {
	return func() tea.Msg {
		if _, _, _, err := validateGuardedAnalysisSafety(cfg); err != nil {
			return purgeResultMsg{err: err}
		}

		// Create backup directory
		backupDir := "deleted_records"
		if err := os.MkdirAll(backupDir, 0o700); err != nil {
			return purgeResultMsg{err: fmt.Errorf("could not create backup dir: %w", err)}
		}

		// Create backup manager
		backupManager := backup.NewPurgedRowManager()

		// Create error handler
		errorHandler := errors.NewErrorHandler(1000)

		// Create interactive purge engine
		purgeConfig := &deletion.InteractivePurgeConfig{
			BackupDir:         backupDir,
			TempDir:           "", // Will create temp dir automatically
			MaxRetries:        3,
			EnableRollback:    true,
			ValidateIntegrity: true,
			ChunkSize:         1000,
		}

		engine, err := deletion.NewInteractivePurgeEngine(
			context.Background(),
			backupManager,
			errorHandler,
			purgeConfig,
		)
		if err != nil {
			return purgeResultMsg{err: fmt.Errorf("failed to create purge engine: %w", err)}
		}
		defer func() {
			if err := engine.Cleanup(); err != nil {
				log.Printf("interactive purge cleanup failed: %v", err)
			}
		}()

		// Process the purge
		result, err := engine.ProcessInteractivePurge(recordsToDelete, purgeConfig)
		if err != nil {
			return purgeResultMsg{err: fmt.Errorf("purge failed: %w", err)}
		}

		// Convert result to purgeResultMsg
		return purgeResultMsg{
			filesModified:  result.ProcessedFiles,
			recordsDeleted: int(result.TotalDeleted),
			err:            nil,
		}
	}
}

func updateProgress(m model) (tea.Model, tea.Cmd) {
	if m.analyser == nil {
		return m, pollProgressCmd(&m)
	}
	processed := m.analyser.ProcessedFiles.Load()
	total := len(m.originalSources)
	percent := 0.0
	if total > 0 {
		percent = float64(processed) / float64(total)
		elapsed := m.totalElapsedTime + time.Since(m.startTime)
		if processed > 10 && percent < 1.0 {
			timePerFile := elapsed / time.Duration(processed)
			remainingFiles := total - int(processed)
			m.eta = time.Duration(remainingFiles) * timePerFile
		}
	}
	folderStr := "Discovering..."
	if f, ok := m.analyser.CurrentFolder.Load().(string); ok && f != "" {
		folderStr = f
	}
	m.status = fmt.Sprintf("Folder: %s | File %d of %d", folderStr, processed, total)
	var cmds []tea.Cmd
	cmds = append(cmds, m.progress.SetPercent(percent))
	if percent < 1.0 && m.viewState == viewProcessing {
		cmds = append(cmds, pollProgressCmd(&m))
	}
	return m, tea.Batch(cmds...)
}

func updateMenu(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.menuCursor > 0 {
				m.menuCursor--
			}
		case keyDown, "j":
			if m.menuCursor < 5 { // Updated to match new menu count
				m.menuCursor++
			}
		case "?":
			m.viewState = viewHelp
		case keyEnter:
			m.analyser = nil
			m.finalReport = nil
			m.originalSources = nil
			m.err = nil

			switch m.menuCursor {
			case 0: // Quick Validation
				m.isValidationRun = true
				m.viewState = viewInputPath
				m.pathInput.Focus()
				return m, textinput.Blink
			case 1: // Full Analysis
				m.isValidationRun = false
				m.viewState = viewInputPath
				m.pathInput.Focus()
				return m, textinput.Blink
			case 2: // Advanced Features
				m.viewState = viewAdvancedMenu
			case 3: // Settings
				m.viewState = viewOptions
			case 4: // Help
				m.viewState = viewHelp
			case 5: // Exit
				m.quitting = true
				return m, tea.Quit
			}
		}
	}
	return m, nil
}

func updateOptions(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.optionsCursor > 0 {
				m.optionsCursor--
			}
		case keyDown, "j":
			if m.optionsCursor < 9 {
				m.optionsCursor++
			}
		case "left":
			if m.optionsCursor == 0 && m.workers > 1 {
				m.workers--
			}
		case "right":
			if m.optionsCursor == 0 {
				m.workers++
			}
		case keyEnter:
			switch m.optionsCursor {
			case 1:
				m.checkKey = !m.checkKey
			case 2:
				m.checkRow = !m.checkRow
			case 3:
				m.showFolderBreakdown = !m.showFolderBreakdown
			case 4:
				m.outputTxt = !m.outputTxt
			case 5:
				m.outputJSON = !m.outputJSON
			case 6:
				m.purgeIDs = !m.purgeIDs
			case 7:
				m.purgeRows = !m.purgeRows
			case 8:
				m.viewState = viewInputLogPath
				m.logPathInput.Focus()
				return m, textinput.Blink
			case 9:
				m.viewState = viewMenu
			}
			return m, saveConfigCmd(m.buildConfig())
		}
	}
	return m, nil
}

func updateInputPath(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.Type == tea.KeyEnter {
			m.path = m.pathInput.Value()
			if m.path == "" {
				m.err = fmt.Errorf("path cannot be empty")
				return m, nil
			}

			paths := strings.Split(m.path, ",")
			for i, p := range paths {
				paths[i] = strings.TrimSpace(p)
				if strings.HasPrefix(paths[i], "gs://") && !m.gcsAvailable {
					m.err = fmt.Errorf("cannot process GCS path: GCS credentials not available")
					return m, nil
				}
			}

			if m.isValidationRun || m.checkKey {
				m.viewState = viewInputKey
				m.pathInput.Blur()
				m.keyInput.Focus()
				return m, textinput.Blink
			}
			m.viewState = viewProcessing
			return m, discoverAllSourcesCmd(m.ctx, paths)
		}
	}
	m.pathInput, cmd = m.pathInput.Update(msg)
	return m, cmd
}

func updateInputKey(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.Type == tea.KeyEnter {
			m.key = m.keyInput.Value()
			if m.key == "" {
				m.err = fmt.Errorf("unique key cannot be empty")
				return m, nil
			}
			m.keyInput.Blur()
			m.viewState = viewProcessing
			paths := strings.Split(m.path, ",")
			for i, p := range paths {
				paths[i] = strings.TrimSpace(p)
			}
			return m, discoverAllSourcesCmd(m.ctx, paths)
		}
	}
	m.keyInput, cmd = m.keyInput.Update(msg)
	return m, cmd
}

func updateInputLogPath(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.Type == tea.KeyEnter {
			m.logPath = m.logPathInput.Value()
			if m.logPath == "" {
				m.err = fmt.Errorf("log path cannot be empty")
				return m, nil
			}
			m.logPathInput.Blur()
			m.viewState = viewOptions
			return m, saveConfigCmd(m.buildConfig())
		}
	}
	m.logPathInput, cmd = m.logPathInput.Update(msg)
	return m, cmd
}

func updateReport(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "r":
			m.wantsToRestart = true
			return m, tea.Quit
		case "n":
			m.wantsToRestart = true
			m.wantsToStartNew = true
			return m, tea.Quit
		case "a":
			if m.finalReport != nil && m.finalReport.Summary.IsValidationReport {
				m.isValidationRun = false
				m.viewState = viewProcessing
				m.totalElapsedTime = 0
				m.wasCancelled = false
				return m, discoverAllSourcesCmd(m.ctx, strings.Split(m.path, ","))
			}
		case "c":
			if m.wasCancelled && m.analyser != nil {
				unprocessedSources := m.analyser.GetUnprocessedSources(m.originalSources)
				if len(unprocessedSources) > 0 {
					m.status = fmt.Sprintf("Continuing analysis on %d remaining files...", len(unprocessedSources))
					m.viewState = viewProcessing
					m.wasCancelled = false
					m.startTime = time.Now()
					m.jobCtx, m.jobCancel = safety.ManagedContext(m.ctx)
					return m, tea.Batch(
						startAnalysisCmd(m.jobCtx, m.analyser, unprocessedSources, m.buildConfig()),
						m.spinner.Tick,
						pollProgressCmd(&m),
					)
				}
			}
		case "p":
			hasIDDupes := m.finalReport != nil && len(m.finalReport.DuplicateIDs) > 0
			hasRowDupes := m.finalReport != nil && len(m.finalReport.DuplicateRows) > 0
			canStartPurge := m.finalReport != nil && !m.finalReport.Summary.IsValidationReport &&
				((m.purgeIDs && hasIDDupes) || (m.purgeRows && hasRowDupes))

			isGCS := strings.Contains(m.path, "gs://")
			if !isGCS && canStartPurge && m.purgeStats.filesModified == 0 {
				if m.purgeIDs && hasIDDupes {
					for k := range m.finalReport.DuplicateIDs {
						m.purgeIDKeys = append(m.purgeIDKeys, k)
					}
					sort.Strings(m.purgeIDKeys)
				}
				if m.purgeRows && hasRowDupes {
					for k := range m.finalReport.DuplicateRows {
						m.purgeRowHashes = append(m.purgeRowHashes, k)
					}
					sort.Strings(m.purgeRowHashes)
				}
				m.viewState = viewPurgeSelection
			}
		}
	}
	return m, nil
}

func updatePurgeSelection(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	var locations []report.LocationInfo
	if m.purgeCursor < len(m.purgeIDKeys) {
		key := m.purgeIDKeys[m.purgeCursor]
		locations = m.finalReport.DuplicateIDs[key]
	} else {
		hash := m.purgeRowHashes[m.purgeCursor-len(m.purgeIDKeys)]
		locations = m.finalReport.DuplicateRows[hash]
	}
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.purgeSelectionCursor > 0 {
				m.purgeSelectionCursor--
			}
		case keyDown, "j":
			if m.purgeSelectionCursor < len(locations)-1 {
				m.purgeSelectionCursor++
			}
		case keyEnter:
			for i, loc := range locations {
				if i != m.purgeSelectionCursor {
					if _, ok := m.recordsToDelete[loc.FilePath]; !ok {
						m.recordsToDelete[loc.FilePath] = make(map[int]bool)
					}
					m.recordsToDelete[loc.FilePath][loc.LineNumber] = true
				}
			}
			m.purgeCursor++
			m.purgeSelectionCursor = 0
			totalToPurge := len(m.purgeIDKeys) + len(m.purgeRowHashes)
			if m.purgeCursor >= totalToPurge {
				m.viewState = viewPurging
				m.status = "Purging records..."
				return m, tea.Batch(performPurgeCmd(m.buildConfig(), m.recordsToDelete), m.spinner.Tick)
			}
		}
	}
	return m, nil
}

func renderMenu(m *model) string {
	var content strings.Builder

	// Enhanced title with better branding
	title := titleStyle.Render("🔍 Data Refinery")
	subtitle := subtitleStyle.Render("Interactive analysis, validation, and reporting")

	content.WriteString(title + "\n")
	content.WriteString(subtitle + "\n\n")

	// Clear description of what the tool does
	description := descriptionStyle.Render(
		"Analyze JSON/NDJSON files for duplicate data, discover schemas,\n" +
			"and review cleanup opportunities across local storage and GCS.")
	content.WriteString(description + "\n\n")

	rewriteNote := cardStyle.Render(
		subtitleStyle.Render("🔁 Rewrite workflow") + "\n\n" +
			"Source rewrites are currently available through the CLI only.\n" +
			"Run `data-refinery rewrite ...` when you want to preview or apply\n" +
			"content changes with backups.")
	content.WriteString(rewriteNote + "\n\n")

	// Enhanced menu options with clear descriptions
	menuOptions := []struct {
		title       string
		description string
		enabled     bool
	}{
		{
			title:       "🔍 Quick Validation",
			description: "Test a unique key across your dataset (fast preview)",
			enabled:     true,
		},
		{
			title:       "🚀 Full Analysis",
			description: "Complete duplicate detection with detailed reports",
			enabled:     true,
		},
		{
			title:       "⚙️ Advanced Features",
			description: "Schema discovery, custom search, selective hashing",
			enabled:     true,
		},
		{
			title:       "🔧 Settings",
			description: "Configure workers, output formats, and options",
			enabled:     true,
		},
		{
			title:       "❓ Help",
			description: "View detailed usage instructions and examples",
			enabled:     true,
		},
		{
			title:       "🚪 Exit",
			description: "Close the application",
			enabled:     true,
		},
	}

	content.WriteString("What would you like to do?\n\n")

	for i, option := range menuOptions {
		var style lipgloss.Style
		cursor := "  "

		if m.menuCursor == i {
			style = menuCursorStyle
			cursor = menuCursorGlyph
		} else {
			style = lipgloss.NewStyle()
		}

		if !option.enabled {
			style = style.Foreground(mutedColor).Strikethrough(true)
		}

		optionLine := style.Render(cursor + option.title)
		descLine := descriptionStyle.Render("    " + option.description)

		content.WriteString(optionLine + "\n")
		content.WriteString(descLine + "\n")
		if i < len(menuOptions)-1 {
			content.WriteString("\n")
		}
	}

	// Enhanced help text with clear keyboard shortcuts
	helpText := helpStyle.Render(
		"\n📋 Navigation:\n" +
			"  ↑/↓ or k/j    Navigate options\n" +
			"  Enter         Select option\n" +
			"  ?             Quick help\n" +
			"  q/Ctrl+C      Exit application\n\n" +
			"💡 Tip: Start with Quick Validation if you're unsure about your data structure.")

	content.WriteString(helpText)

	return content.String()
}

func renderOptions(m *model) string {
	var content strings.Builder

	title := titleStyle.Render("⚙️ Settings & Configuration")
	content.WriteString(title + "\n\n")

	description := descriptionStyle.Render(
		"Configure analysis options, output settings, and performance parameters.")
	content.WriteString(description + "\n\n")

	// Group options into logical sections
	sections := []struct {
		title   string
		options []struct {
			name        string
			value       string
			description string
			index       int
		}
	}{
		{
			title: "📊 Analysis Settings",
			options: []struct {
				name        string
				value       string
				description string
				index       int
			}{
				{
					name:        "Workers",
					value:       fmt.Sprintf("%d", m.workers),
					description: "Number of concurrent processing threads",
					index:       0,
				},
				{
					name:        "Check Duplicate Keys",
					value:       formatBool(m.checkKey),
					description: "Detect records with duplicate unique identifiers",
					index:       1,
				},
				{
					name:        "Check Duplicate Rows",
					value:       formatBool(m.checkRow),
					description: "Detect completely identical records",
					index:       2,
				},
			},
		},
		{
			title: "📋 Report Settings",
			options: []struct {
				name        string
				value       string
				description string
				index       int
			}{
				{
					name:        "Show Folder Breakdown",
					value:       formatBool(m.showFolderBreakdown),
					description: "Include per-folder statistics in reports",
					index:       3,
				},
				{
					name:        "Enable TXT Reports",
					value:       formatBool(m.outputTxt),
					description: "Generate human-readable text reports",
					index:       4,
				},
				{
					name:        "Enable JSON Reports",
					value:       formatBool(m.outputJSON),
					description: "Generate machine-readable JSON reports",
					index:       5,
				},
			},
		},
		{
			title: "🗑️ Cleanup Options",
			options: []struct {
				name        string
				value       string
				description string
				index       int
			}{
				{
					name:        "Allow ID Purging",
					value:       formatBool(m.purgeIDs),
					description: "Enable interactive removal of duplicate IDs",
					index:       6,
				},
				{
					name:        "Allow Row Purging",
					value:       formatBool(m.purgeRows),
					description: "Enable interactive removal of duplicate rows",
					index:       7,
				},
			},
		},
		{
			title: "📁 File Settings",
			options: []struct {
				name        string
				value       string
				description string
				index       int
			}{
				{
					name:        "Log/Report Directory",
					value:       m.logPath,
					description: "Directory where logs and reports are saved",
					index:       8,
				},
			},
		},
	}

	currentSection := 0

	// Find which section we're in
	for sectionIdx, section := range sections {
		for _, option := range section.options {
			if option.index == m.optionsCursor {
				currentSection = sectionIdx
				break
			}
		}
	}
	// Render sections
	for sectionIdx, section := range sections {
		sectionStyle := subtitleStyle
		if sectionIdx == currentSection {
			sectionStyle = sectionStyle.Foreground(primaryColor)
		}

		content.WriteString(sectionStyle.Render(section.title) + "\n")

		for optionIdx, option := range section.options {
			cursor := "  "
			style := lipgloss.NewStyle()

			if option.index == m.optionsCursor {
				cursor = menuCursorGlyph
				style = menuCursorStyle
			}

			optionLine := style.Render(fmt.Sprintf("%s%s: %s", cursor, option.name, option.value))
			descLine := descriptionStyle.Render("    " + option.description)

			content.WriteString(optionLine + "\n")
			content.WriteString(descLine + "\n")
			if optionIdx < len(section.options)-1 {
				content.WriteString("\n")
			}
		}

		if sectionIdx < len(sections)-1 {
			content.WriteString("\n")
		}
	}

	// Back option
	cursor := "  "
	style := lipgloss.NewStyle()
	if m.optionsCursor == 9 {
		cursor = menuCursorGlyph
		style = menuCursorStyle
	}

	backLine := style.Render(cursor + "← Back to Main Menu")
	content.WriteString("\n" + backLine + "\n")

	// Enhanced help text
	helpText := helpStyle.Render(
		"\n📋 Navigation:\n" +
			"  ↑/↓ or k/j    Navigate options\n" +
			"  ←/→ or h/l    Change numeric values\n" +
			"  Enter         Toggle boolean values or edit paths\n" +
			"  Esc           Return to main menu\n\n" +
			"💡 Tip: Changes are saved automatically. Use more workers for better performance on large datasets.")

	content.WriteString(helpText)

	return content.String()
}

func formatBool(b bool) string {
	if b {
		return successStyle.Render("✓ Enabled")
	}
	return warningStyle.Render("✗ Disabled")
}

func renderHelp(m *model) string {
	var content strings.Builder

	title := titleStyle.Render("❓ Help & Usage Guide")
	content.WriteString(title + "\n\n")

	// Quick start section
	quickStart := cardStyle.Render(
		subtitleStyle.Render("🚀 Quick Start") + "\n\n" +
			"1. Choose 'Quick Validation' to test a unique key\n" +
			"2. Enter your data path (local or gs://bucket/path)\n" +
			"3. Specify the record key for uniqueness (for example 'id' or 'user_id')\n" +
			"4. Review the validation results\n" +
			"5. Run 'Full Analysis' for complete duplicate detection")
	content.WriteString(quickStart + "\n")

	// Supported formats section
	formats := cardStyle.Render(
		subtitleStyle.Render("📄 Supported Formats") + "\n\n" +
			"• CSV files (.csv)\n" +
			"• TSV files (.tsv)\n" +
			"• XLSX workbooks (.xlsx)\n" +
			"• XML documents (.xml)\n" +
			"• JSON files (.json)\n" +
			"• NDJSON/JSONL files (.jsonl, .ndjson)\n" +
			"• Local filesystem paths\n" +
			"• Google Cloud Storage (gs://bucket/path)\n" +
			"• Multiple comma-separated paths")
	content.WriteString(formats + "\n")

	// Advanced features section
	advanced := cardStyle.Render(
		subtitleStyle.Render("⚙️ Advanced Features") + "\n\n" +
			"• Schema Discovery: Analyze data structure and types\n" +
			"• Custom Search: Define complex search patterns\n" +
			"• Selective Hashing: Include/exclude specific fields\n" +
			"• Deletion Rules: Automated cleanup based on criteria\n" +
			"• Batch Processing: Handle large datasets efficiently")
	content.WriteString(advanced + "\n")

	// Keyboard shortcuts section
	shortcuts := cardStyle.Render(
		subtitleStyle.Render("⌨️ Keyboard Shortcuts") + "\n\n" +
			"Navigation:\n" +
			"  ↑/↓ or k/j    Move up/down in menus\n" +
			"  ←/→ or h/l    Change numeric values\n" +
			"  Enter         Select/toggle options\n" +
			"  Esc           Go back to previous screen\n" +
			"  Tab           Cycle through options\n\n" +
			"Global:\n" +
			"  ?             Show help (from main menu)\n" +
			"  q/Ctrl+C      Exit application\n" +
			"  r             Restart analysis (from results)\n" +
			"  n             Start new analysis (from results)")
	content.WriteString(shortcuts + "\n")

	// Command line section
	if m.gcsAvailable {
		cmdLine := cardStyle.Render(
			subtitleStyle.Render("💻 Command Line Usage") + "\n\n" +
				"Headless mode (no TUI):\n" +
				"  --headless                 Run without interactive interface\n" +
				"  --path /data,gs://bucket   Comma-separated paths\n" +
				"  --key id                   Unique key field name\n" +
				"  --workers 8                Number of concurrent workers\n" +
				"  --output json              Output format (txt/json)\n" +
				"  --validate                 Quick validation mode\n" +
				"  --xml-record-path a.b.c    Stream repeated XML elements as records\n\n" +
				"Examples:\n" +
				"  ./data-refinery --path /data --key user_id\n" +
				"  ./data-refinery --headless --path gs://bucket/data --key id --output json\n" +
				"  ./data-refinery rewrite --path gs://bucket/data --top-level-key id --top-level-vals ids.csv --mode preview")
		content.WriteString(cmdLine + "\n")
	}

	// Tips section
	tips := cardStyle.Render(
		subtitleStyle.Render("💡 Tips & Best Practices") + "\n\n" +
			"• Start with Quick Validation on a small sample\n" +
			"• Use more workers for better performance on large datasets\n" +
			"• Enable both TXT and JSON outputs for flexibility\n" +
			"• Review folder breakdown to identify problem areas\n" +
			"• Use Advanced Features for complex data structures\n" +
			"• Always backup data before using purge features")
	content.WriteString(tips + "\n")

	// Footer
	footer := helpStyle.Render(
		"Press any key to return to the main menu")
	content.WriteString(footer)

	return content.String()
}

func renderInputPath(m *model) string {
	var content strings.Builder

	title := titleStyle.Render("📂 Data Path Configuration")
	content.WriteString(title + "\n\n")

	// Context-aware description
	var description string
	if m.isValidationRun {
		description = "Enter the path(s) to your data for validation testing.\n" +
			"This will quickly check if your specified unique key exists and is valid."
	} else {
		description = "Enter the path(s) to your data for complete duplicate analysis.\n" +
			"This will perform comprehensive duplicate detection across all records."
	}

	content.WriteString(descriptionStyle.Render(description) + "\n\n")

	// Input section with enhanced styling
	inputLabel := subtitleStyle.Render("Data Path:")
	content.WriteString(inputLabel + "\n")

	// Enhanced input with better visual feedback
	inputBox := inputStyle.Render(m.pathInput.View())
	content.WriteString(inputBox + "\n\n")

	// Format support info
	formatInfo := cardStyle.Render(
		subtitleStyle.Render("📄 Supported Formats") + "\n\n" +
			"• Local paths: /path/to/data, ./relative/path\n" +
			"• Multiple paths: /path1,/path2,/path3\n" +
			"• Supported files: .csv, .tsv, .xlsx, .xml, .json, .jsonl, .ndjson\n")

	// Add GCS info if available
	if m.gcsAvailable {
		gcsInfo := cardStyle.Render(
			subtitleStyle.Render("☁️ Google Cloud Storage") + "\n\n" +
				"• GCS paths: gs://bucket/path\n" +
				"• Mixed paths: /local/path,gs://bucket/path\n" +
				"• Authentication detected and ready")
		content.WriteString(formatInfo + "\n" + gcsInfo + "\n")
	} else {
		noGcsInfo := cardStyle.Render(
			subtitleStyle.Render("☁️ Google Cloud Storage") + "\n\n" +
				warningStyle.Render("• GCS authentication not available\n") +
				"• Only local paths will be processed")
		content.WriteString(formatInfo + "\n" + noGcsInfo + "\n")
	}

	// Real-time validation feedback
	if m.pathInput.Value() != "" {
		paths := strings.Split(m.pathInput.Value(), ",")
		validationInfo := validatePaths(paths, m.gcsAvailable)
		content.WriteString(validationInfo + "\n")
	}

	// Enhanced help text
	helpText := helpStyle.Render(
		"📋 Controls:\n" +
			"  Enter         Continue to next step\n" +
			"  Esc           Return to main menu\n" +
			"  Ctrl+C        Exit application\n\n" +
			"💡 Tip: Use absolute paths for best results. Relative paths are resolved from current directory.")

	content.WriteString(helpText)

	return content.String()
}

// Helper function for path validation.
func validatePaths(paths []string, gcsAvailable bool) string {
	var validPaths, invalidPaths, gcsWarnings []string

	for _, path := range paths {
		trimmed := strings.TrimSpace(path)
		if trimmed == "" {
			continue
		}

		if strings.HasPrefix(trimmed, "gs://") {
			if gcsAvailable {
				validPaths = append(validPaths, trimmed)
				continue
			}
			gcsWarnings = append(gcsWarnings, trimmed)
			continue
		}

		if strings.Contains(trimmed, "..") {
			invalidPaths = append(invalidPaths, trimmed+" (contains '..')")
			continue
		}

		validPaths = append(validPaths, trimmed)
	}

	var content strings.Builder

	if len(validPaths) > 0 {
		content.WriteString(successStyle.Render("✓ Valid paths: ") + fmt.Sprintf("%d\n", len(validPaths)))
	}

	if len(gcsWarnings) > 0 {
		content.WriteString(warningStyle.Render("⚠ GCS paths (auth unavailable): ") + fmt.Sprintf("%d\n", len(gcsWarnings)))
	}

	if len(invalidPaths) > 0 {
		content.WriteString(errorStyle.Render("✗ Invalid paths: ") + fmt.Sprintf("%d\n", len(invalidPaths)))
		for _, invalid := range invalidPaths {
			content.WriteString(errorStyle.Render("  • ") + invalid + "\n")
		}
	}

	return content.String()
}

func renderInputLogPath(m *model) string {
	pad := strings.Repeat(" ", 2)
	help := helpStyle.Render("Press Enter to submit, 'q' or 'ctrl+c' to quit, 'esc' to go back.")
	return fmt.Sprintf("\n%sPlease enter the path for logs and reports:\n\n%s%s\n\n%s", pad, pad, m.logPathInput.View(), help)
}

func renderInputKey(m *model) string {
	var content strings.Builder

	title := titleStyle.Render("🔑 Unique Key Configuration")
	content.WriteString(title + "\n\n")

	// Show selected paths for context
	pathInfo := cardStyle.Render(
		subtitleStyle.Render("📂 Selected Paths") + "\n\n" +
			m.path)
	content.WriteString(pathInfo + "\n\n")

	// Description based on mode
	var description string
	if m.isValidationRun {
		description = "Specify the JSON key that should be unique across all records.\n" +
			"This validation will check if the key exists and identify any duplicates."
	} else {
		description = "Specify the JSON key that should be unique across all records.\n" +
			"This will be used for comprehensive duplicate detection and analysis."
	}

	content.WriteString(descriptionStyle.Render(description) + "\n\n")

	// Input section
	inputLabel := subtitleStyle.Render("Unique Key Field:")
	content.WriteString(inputLabel + "\n")

	inputBox := inputStyle.Render(m.keyInput.View())
	content.WriteString(inputBox + "\n\n")

	// Examples and guidance
	examples := cardStyle.Render(
		subtitleStyle.Render("📋 Common Examples") + "\n\n" +
			"• Simple keys: id, user_id, product_id, uuid\n" +
			"• Nested keys: user.id, metadata.identifier\n" +
			"• Array keys: items[0].id, users[*].email\n" +
			"• Complex paths: data.records[*].unique_id")
	content.WriteString(examples + "\n")

	// Real-time validation
	if m.keyInput.Value() != "" {
		keyValidation := validateKeyInput(m.keyInput.Value())
		content.WriteString(keyValidation + "\n")
	}

	// Help text
	helpText := helpStyle.Render(
		"📋 Controls:\n" +
			"  Enter         Start analysis\n" +
			"  Esc           Return to path selection\n" +
			"  Ctrl+C        Exit application\n\n" +
			"💡 Tip: The key must exist in your JSON data. Use nested notation (e.g., 'user.id') for nested fields.")

	content.WriteString(helpText)

	return content.String()
}

// Helper function for key validation.
func validateKeyInput(key string) string {
	var content strings.Builder

	// Basic validation
	if key == "" {
		return ""
	}

	// Check for common patterns
	if strings.Contains(key, " ") {
		content.WriteString(warningStyle.Render("⚠ Key contains spaces - ensure this is correct\n"))
	}

	if strings.Contains(key, "[*]") {
		content.WriteString(successStyle.Render("✓ Array notation detected - will check all array elements\n"))
	}

	if strings.Contains(key, ".") {
		content.WriteString(successStyle.Render("✓ Nested key detected - will traverse object hierarchy\n"))
	}

	// Length validation
	if len(key) > 100 {
		content.WriteString(warningStyle.Render("⚠ Key is very long - ensure this is correct\n"))
	}

	// Special characters
	if strings.ContainsAny(key, "{}[]()=+") {
		content.WriteString(warningStyle.Render("⚠ Special characters detected - verify syntax\n"))
	}

	if content.Len() == 0 {
		content.WriteString(successStyle.Render("✓ Key format looks valid\n"))
	}

	return content.String()
}

func renderProcessing(m *model) string {
	pad := strings.Repeat(" ", 2)
	var progressView, timingView string
	if m.processing {
		progressView = "\n" + m.progress.View()
		elapsedStr := (m.totalElapsedTime + time.Since(m.startTime)).Round(time.Second).String()
		etaStr := m.eta.Round(time.Second).String()
		timingView = timingStyle.Render(fmt.Sprintf(" (Elapsed: %s, ETA: %s)", elapsedStr, etaStr))
	}
	status := statusStyle.Render(m.status)
	if m.viewState == viewCancelling {
		return fmt.Sprintf("\n%s%s %s\n", pad, m.spinner.View(), m.status)
	}
	return fmt.Sprintf("\n%s%s%s%s\n%s", pad, m.spinner.View(), status, timingView, progressView) + helpStyle.Render("\nPress 'q' or 'ctrl+c' to cancel.")
}

func renderReport(m *model) string {
	if m.finalReport == nil {
		return "Generating report..."
	}
	var b strings.Builder
	b.WriteString("\n" + m.finalReport.String(false, m.checkKey, m.checkRow, m.showFolderBreakdown))
	if m.purgeStats.filesModified > 0 || m.purgeStats.recordsDeleted > 0 {
		purgeSummary := fmt.Sprintf("Files Modified: %d\nRecords Deleted: %d (and backed up)", m.purgeStats.filesModified, m.purgeStats.recordsDeleted)
		b.WriteString("\n\n" + reportStyle.Render(purgeSummary))
	} else if m.purgeStats.err != nil {
		b.WriteString("\n\n" + errorStyle.Render("Purge failed: "+m.purgeStats.err.Error()))
	}
	if !m.finalReport.Summary.IsValidationReport && (m.outputTxt || m.outputJSON) {
		b.WriteString("\n\n" + fmt.Sprintf("Reports saved to files with extension(s): %s", m.savedFilename))
	}

	helpParts := []string{}
	if m.finalReport != nil && m.finalReport.Summary.IsValidationReport {
		helpParts = append(helpParts, "(a)nalyse now")
	}
	if m.wasCancelled {
		helpParts = append(helpParts, "(c)ontinue")
	}
	helpParts = append(helpParts, "(r)estart", "(n)ew job")

	hasIDDupesToPurge := m.purgeIDs && m.finalReport != nil && len(m.finalReport.DuplicateIDs) > 0
	hasRowDupesToPurge := m.purgeRows && m.finalReport != nil && len(m.finalReport.DuplicateRows) > 0
	canDisplayPurge := m.finalReport != nil && !m.finalReport.Summary.IsValidationReport && (hasIDDupesToPurge || hasRowDupesToPurge)

	isGCS := strings.Contains(m.path, "gs://")
	if !isGCS && canDisplayPurge && m.purgeStats.filesModified == 0 {
		helpParts = append(helpParts, "(p)urge")
	}
	helpParts = append(helpParts, "(q)uit")

	b.WriteString("\n" + helpStyle.Render("Press "+strings.Join(helpParts, ", ")+"."))
	return b.String()
}

func renderPurgeSelection(m *model) string {
	var b strings.Builder
	var locations []report.LocationInfo
	var title string
	totalToPurge := len(m.purgeIDKeys) + len(m.purgeRowHashes)
	isPurgingIDs := m.purgeCursor < len(m.purgeIDKeys)
	if isPurgingIDs {
		key := m.purgeIDKeys[m.purgeCursor]
		locations = m.finalReport.DuplicateIDs[key]
		title = fmt.Sprintf("Duplicate ID '%s'", key)
	} else {
		hash := m.purgeRowHashes[m.purgeCursor-len(m.purgeIDKeys)]
		locations = m.finalReport.DuplicateRows[hash]
		title = fmt.Sprintf("Duplicate Row (hash %s...)", hash[:8])
	}
	fmt.Fprintf(&b, "Resolving %d of %d duplicate sets...\n", m.purgeCursor+1, totalToPurge)
	b.WriteString(headerStyle.Render(title) + "\n\n")
	b.WriteString("Select the one record to KEEP:\n")
	for i, loc := range locations {
		cursor := "  "
		if i == m.purgeSelectionCursor {
			cursor = selectionStyle.Render("> ")
		}
		fmt.Fprintf(&b, "%sFile: %s\n  Line: %d\n", cursor, loc.FilePath, loc.LineNumber)
	}
	b.WriteString(helpStyle.Render("\nUse up/down arrows to select. Enter to confirm and move to next set."))
	return b.String()
}

// Advanced Features Implementation

func updateAdvancedMenu(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.advancedMenuCursor > 0 {
				m.advancedMenuCursor--
			}
		case keyDown, "j":
			if m.advancedMenuCursor < 5 { // Updated to match new menu count
				m.advancedMenuCursor++
			}
		case keyEnter:
			switch m.advancedMenuCursor {
			case 0: // Custom Search Patterns
				m.viewState = viewSearchConfig
			case 1: // Schema Discovery
				m.viewState = viewSchemaConfig
			case 2: // Selective Hashing
				m.viewState = viewHashingConfig
			case 3: // Auto-Cleanup Rules
				if len(m.searchTargets) > 0 { // Only allow if search targets exist
					m.viewState = viewDeletionConfig
				}
			case 4: // Save Configuration
				if m.advancedEnabled {
					return m, saveConfigCmd(m.buildConfig())
				}
			case 5: // Back to Main Menu
				m.viewState = viewMenu
			}
		}
	}
	return m, nil
}

func updateSchemaConfig(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.schemaConfigCursor > 0 {
				m.schemaConfigCursor--
			}
		case keyDown, "j":
			if m.schemaConfigCursor < 3 { // Updated to match new menu count
				m.schemaConfigCursor++
			}
		case keyEnter:
			switch m.schemaConfigCursor {
			case 0: // Toggle Schema Discovery
				m.schemaDiscovery.Enabled = !m.schemaDiscovery.Enabled
				m.advancedEnabled = true
			case 1: // Quick Setup
				m.schemaDiscovery.Enabled = true
				m.schemaDiscovery.SamplePercent = 0.1
				m.schemaDiscovery.MaxDepth = 10
				m.schemaDiscovery.MaxSamples = 100000
				m.schemaDiscovery.OutputFormats = []string{"json"}
				m.schemaDiscovery.GroupByFolder = false
				m.advancedEnabled = true
			case 2: // Custom Configuration - could expand this later
				// For now, just cycle through some common configurations
				switch m.schemaDiscovery.SamplePercent {
				case 0.1:
					m.schemaDiscovery.SamplePercent = 0.05
				case 0.05:
					m.schemaDiscovery.SamplePercent = 0.2
				default:
					m.schemaDiscovery.SamplePercent = 0.1
				}
				m.advancedEnabled = true
			case 3: // Back to Advanced Menu
				m.viewState = viewAdvancedMenu
			}
		}
	}
	return m, nil
}

func updateSchemaResult(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg.(type) {
	case tea.KeyMsg:
		m.viewState = viewSchemaConfig
	}
	return m, nil
}

func updateSearchConfig(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.searchConfigCursor > 0 {
				m.searchConfigCursor--
			}
		case keyDown, "j":
			maxCursor := len(m.searchTargets) + 1
			if m.searchConfigCursor < maxCursor {
				m.searchConfigCursor++
			}
		case keyEnter:
			if m.searchConfigCursor == len(m.searchTargets) {
				// Add new target
				m.editingSearchTarget = &config.SearchTarget{
					Name:          "",
					Type:          "direct",
					Path:          "",
					TargetValues:  []string{},
					CaseSensitive: false,
				}
				m.editingTargetCursor = 0
				m.searchNameInput.Focus()
				m.viewState = viewSearchTargetEdit
				return m, textinput.Blink
			} else if m.searchConfigCursor == len(m.searchTargets)+1 {
				// Back
				m.viewState = viewAdvancedMenu
			} else {
				// Edit existing target
				m.editingSearchTarget = &m.searchTargets[m.searchConfigCursor]
				m.editingTargetCursor = 0
				m.searchNameInput.SetValue(m.editingSearchTarget.Name)
				m.searchNameInput.Focus()
				m.viewState = viewSearchTargetEdit
				return m, textinput.Blink
			}
		case "d":
			if m.searchConfigCursor < len(m.searchTargets) {
				// Delete target
				m.searchTargets = append(m.searchTargets[:m.searchConfigCursor], m.searchTargets[m.searchConfigCursor+1:]...)
				if m.searchConfigCursor >= len(m.searchTargets) && m.searchConfigCursor > 0 {
					m.searchConfigCursor--
				}
				m.advancedEnabled = true
			}
		}
	}
	return m, nil
}

func updateSearchTargetEdit(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.editingTargetCursor > 0 {
				m.editingTargetCursor--
				m.focusSearchInput()
				return m, textinput.Blink
			}
		case keyDown, "j":
			if m.editingTargetCursor < 4 {
				m.editingTargetCursor++
				m.focusSearchInput()
				return m, textinput.Blink
			}
		case keyEnter:
			if m.editingTargetCursor == 4 {
				// Save
				m.editingSearchTarget.Name = m.searchNameInput.Value()
				m.editingSearchTarget.Path = m.searchPathInput.Value()
				valuesStr := m.searchValuesInput.Value()
				if valuesStr != "" {
					m.editingSearchTarget.TargetValues = strings.Split(valuesStr, ",")
					for i, v := range m.editingSearchTarget.TargetValues {
						m.editingSearchTarget.TargetValues[i] = strings.TrimSpace(v)
					}
				}

				// Add to targets if new
				found := false
				for i, t := range m.searchTargets {
					if t.Name == m.editingSearchTarget.Name {
						m.searchTargets[i] = *m.editingSearchTarget
						found = true
						break
					}
				}
				if !found {
					m.searchTargets = append(m.searchTargets, *m.editingSearchTarget)
				}

				m.advancedEnabled = true
				m.viewState = viewSearchConfig
				m.searchNameInput.Blur()
				m.searchPathInput.Blur()
				m.searchValuesInput.Blur()
				return m, nil
			}
		case "tab":
			if m.editingTargetCursor == 1 {
				// Cycle through types
				types := []string{"direct", "nested_array", "nested_object", "jsonpath"}
				currentIndex := 0
				for i, t := range types {
					if t == m.editingSearchTarget.Type {
						currentIndex = i
						break
					}
				}
				m.editingSearchTarget.Type = types[(currentIndex+1)%len(types)]
			} else if m.editingTargetCursor == 3 {
				// Toggle case sensitivity
				m.editingSearchTarget.CaseSensitive = !m.editingSearchTarget.CaseSensitive
			}
		}
	}

	// Update the focused input
	switch m.editingTargetCursor {
	case 0:
		m.searchNameInput, cmd = m.searchNameInput.Update(msg)
	case 2:
		m.searchPathInput, cmd = m.searchPathInput.Update(msg)
	case 4:
		m.searchValuesInput, cmd = m.searchValuesInput.Update(msg)
	}

	return m, cmd
}

func (m *model) focusSearchInput() {
	m.searchNameInput.Blur()
	m.searchPathInput.Blur()
	m.searchValuesInput.Blur()

	switch m.editingTargetCursor {
	case 0:
		m.searchNameInput.Focus()
	case 2:
		m.searchPathInput.Focus()
	case 4:
		m.searchValuesInput.Focus()
	}
}

func updateHashingConfig(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.hashingConfigCursor > 0 {
				m.hashingConfigCursor--
			}
		case keyDown, "j":
			if m.hashingConfigCursor < 3 {
				m.hashingConfigCursor++
			}
		case keyEnter:
			switch m.hashingConfigCursor {
			case 0: // Hashing mode
				modes := []string{"full_row", hashModeSelective, hashModeExcludeKeys}
				currentIndex := 0
				for i, mode := range modes {
					if mode == m.hashingStrategy.Mode {
						currentIndex = i
						break
					}
				}
				m.hashingStrategy.Mode = modes[(currentIndex+1)%len(modes)]
				m.advancedEnabled = true
			case 1: // Include keys (for selective mode)
				if m.hashingStrategy.Mode == hashModeSelective {
					m.hashKeysInput.SetValue(strings.Join(m.hashingStrategy.IncludeKeys, ","))
					m.hashKeysInput.Focus()
					return m, textinput.Blink
				}
			case 2: // Exclude keys (for exclude_keys mode)
				if m.hashingStrategy.Mode == hashModeExcludeKeys {
					m.hashKeysInput.SetValue(strings.Join(m.hashingStrategy.ExcludeKeys, ","))
					m.hashKeysInput.Focus()
					return m, textinput.Blink
				}
			case 3: // Back
				m.viewState = viewAdvancedMenu
			}
		}
	}

	// Handle key input
	var cmd tea.Cmd
	m.hashKeysInput, cmd = m.hashKeysInput.Update(msg)

	if msg, ok := msg.(tea.KeyMsg); ok && msg.Type == tea.KeyEnter {
		keysStr := m.hashKeysInput.Value()
		if keysStr != "" {
			keys := strings.Split(keysStr, ",")
			for i, k := range keys {
				keys[i] = strings.TrimSpace(k)
			}
			switch m.hashingStrategy.Mode {
			case hashModeSelective:
				m.hashingStrategy.IncludeKeys = keys
			case hashModeExcludeKeys:
				m.hashingStrategy.ExcludeKeys = keys
			}
			m.advancedEnabled = true
		}
		m.hashKeysInput.Blur()
		m.hashKeysInput.SetValue("")
	}

	return m, cmd
}

func updateDeletionConfig(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.deletionConfigCursor > 0 {
				m.deletionConfigCursor--
			}
		case keyDown, "j":
			maxCursor := len(m.deletionRules) + 1
			if m.deletionConfigCursor < maxCursor {
				m.deletionConfigCursor++
			}
		case keyEnter:
			if m.deletionConfigCursor == len(m.deletionRules) {
				// Add new rule
				m.editingDeletionRule = &config.DeletionRule{
					SearchTarget: "",
					Action:       "delete_row",
					OutputPath:   "",
				}
				m.editingRuleCursor = 0
				m.viewState = viewDeletionRuleEdit
			} else if m.deletionConfigCursor == len(m.deletionRules)+1 {
				// Back
				m.viewState = viewAdvancedMenu
			} else {
				// Edit existing rule
				m.editingDeletionRule = &m.deletionRules[m.deletionConfigCursor]
				m.editingRuleCursor = 0
				m.viewState = viewDeletionRuleEdit
			}
		case "d":
			if m.deletionConfigCursor < len(m.deletionRules) {
				// Delete rule
				m.deletionRules = append(m.deletionRules[:m.deletionConfigCursor], m.deletionRules[m.deletionConfigCursor+1:]...)
				if m.deletionConfigCursor >= len(m.deletionRules) && m.deletionConfigCursor > 0 {
					m.deletionConfigCursor--
				}
				m.advancedEnabled = true
			}
		}
	}
	return m, nil
}

func updateDeletionRuleEdit(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.editingRuleCursor > 0 {
				m.editingRuleCursor--
			}
		case keyDown, "j":
			if m.editingRuleCursor < 3 {
				m.editingRuleCursor++
			}
		case keyEnter:
			if m.editingRuleCursor == 3 {
				// Save
				m.editingDeletionRule.OutputPath = m.ruleOutputPathInput.Value()

				// Add to rules if new
				found := false
				for i, r := range m.deletionRules {
					if r.SearchTarget == m.editingDeletionRule.SearchTarget {
						m.deletionRules[i] = *m.editingDeletionRule
						found = true
						break
					}
				}
				if !found {
					m.deletionRules = append(m.deletionRules, *m.editingDeletionRule)
				}

				m.advancedEnabled = true
				m.viewState = viewDeletionConfig
				m.ruleOutputPathInput.Blur()
				return m, nil
			}
		case "tab":
			cycleDeletionRuleField(&m)
		}
	}

	// Update output path input
	if m.editingRuleCursor == 2 {
		m.ruleOutputPathInput, cmd = m.ruleOutputPathInput.Update(msg)
	}

	return m, cmd
}

func updateAdvancedOutput(m model, msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "up", "k":
			if m.outputConfigCursor > 0 {
				m.outputConfigCursor--
			}
		case keyDown, "j":
			if m.outputConfigCursor < 2 {
				m.outputConfigCursor++
			}
		case keyEnter:
			switch m.outputConfigCursor {
			case 0: // Toggle advanced enabled
				m.advancedEnabled = !m.advancedEnabled
			case 1: // Save advanced config
				return m, saveConfigCmd(m.buildConfig())
			case 2: // Back
				m.viewState = viewAdvancedMenu
			}
		}
	}
	return m, nil
}

func handleGlobalKey(m model, msg tea.KeyMsg) (tea.Model, tea.Cmd, bool) {
	if m.err != nil {
		m.err = nil
		m.viewState = viewMenu
		return m, nil, true
	}

	if msg.String() != "ctrl+c" && msg.String() != "q" {
		return m, nil, false
	}

	if m.viewState == viewProcessing {
		return cancelProcessing(m), nil, true
	}
	if m.viewState == viewCancelling || m.viewState == viewPurging {
		return m, nil, true
	}

	shutdownModel(&m)
	return m, tea.Quit, true
}

func cancelProcessing(m model) model {
	m.status = "Cancelling... generating partial report."
	m.viewState = viewCancelling
	m.wasCancelled = true
	if !m.startTime.IsZero() {
		m.totalElapsedTime += time.Since(m.startTime)
		m.startTime = time.Time{}
	}
	if m.jobCancel != nil {
		m.jobCancel()
	}
	return m
}

func shutdownModel(m *model) {
	m.quitting = true
	if m.jobCancel != nil {
		m.jobCancel()
	}
	if m.stateManager != nil {
		if err := m.stateManager.Close(); err != nil {
			log.Printf("state manager close failed: %v", err)
		}
	}
	if m.memoryManager != nil {
		m.memoryManager.Stop()
	}
}

func cycleDeletionRuleField(m *model) {
	switch m.editingRuleCursor {
	case 0:
		cycleDeletionSearchTarget(m)
	case 1:
		cycleDeletionAction(m)
	}
}

func cycleDeletionSearchTarget(m *model) {
	if len(m.searchTargets) == 0 {
		return
	}

	currentIndex := -1
	for i, target := range m.searchTargets {
		if target.Name == m.editingDeletionRule.SearchTarget {
			currentIndex = i
			break
		}
	}

	nextIndex := (currentIndex + 1) % len(m.searchTargets)
	m.editingDeletionRule.SearchTarget = m.searchTargets[nextIndex].Name
}

func cycleDeletionAction(m *model) {
	actions := []string{"delete_row", "delete_matches", "mark_for_deletion"}
	currentIndex := 0
	for i, action := range actions {
		if action == m.editingDeletionRule.Action {
			currentIndex = i
			break
		}
	}

	m.editingDeletionRule.Action = actions[(currentIndex+1)%len(actions)]
}

// Render functions for advanced features

func renderAdvancedMenu(m *model) string {
	var content strings.Builder

	title := titleStyle.Render("⚙️ Advanced Features")
	content.WriteString(title + "\n\n")

	// Simple, focused description
	description := descriptionStyle.Render(
		"Configure advanced data processing options for complex analysis needs.")
	content.WriteString(description + "\n\n")

	// Simplified menu with clear focus
	menuOptions := []struct {
		title       string
		description string
		status      string
		enabled     bool
	}{
		{
			title:       "🔍 Custom Search Patterns",
			description: "Define specific data patterns to search for",
			status:      fmt.Sprintf("(%d configured)", len(m.searchTargets)),
			enabled:     true,
		},
		{
			title:       "🏗️ Schema Discovery",
			description: "Analyze data structure and field types",
			status:      fmt.Sprintf("(%s)", getStatusText(m.schemaDiscovery.Enabled)),
			enabled:     true,
		},
		{
			title:       "🔐 Selective Hashing",
			description: "Control which fields are used for duplicate detection",
			status:      fmt.Sprintf("(%s mode)", m.hashingStrategy.Mode),
			enabled:     true,
		},
		{
			title:       "🧹 Auto-Cleanup Rules",
			description: "Set up automated data cleanup based on search results",
			status:      fmt.Sprintf("(%d rules)", len(m.deletionRules)),
			enabled:     len(m.searchTargets) > 0, // Only enable if search targets exist
		},
		{
			title:       "💾 Save Configuration",
			description: "Save current advanced settings for future use",
			status:      "",
			enabled:     m.advancedEnabled,
		},
		{
			title:       "← Back to Main Menu",
			description: "Return to the main application menu",
			status:      "",
			enabled:     true,
		},
	}

	for i, option := range menuOptions {
		var style lipgloss.Style
		cursor := "  "

		if m.advancedMenuCursor == i {
			cursor = menuCursorGlyph
			style = menuCursorStyle
		} else {
			style = lipgloss.NewStyle()
		}

		if !option.enabled {
			style = style.Foreground(mutedColor)
			cursor = "  "
		}

		optionLine := style.Render(cursor + option.title)
		if option.status != "" {
			optionLine += " " + lipgloss.NewStyle().Foreground(mutedColor).Render(option.status)
		}

		descLine := descriptionStyle.Render("    " + option.description)

		content.WriteString(optionLine + "\n")
		if option.enabled {
			content.WriteString(descLine + "\n")
		}

		if i < len(menuOptions)-1 {
			content.WriteString("\n")
		}
	}

	// Show current configuration summary
	if m.advancedEnabled {
		summary := cardStyle.Render(
			subtitleStyle.Render("📋 Current Configuration") + "\n" +
				fmt.Sprintf("• Search Targets: %d\n", len(m.searchTargets)) +
				fmt.Sprintf("• Hashing Mode: %s\n", m.hashingStrategy.Mode) +
				fmt.Sprintf("• Schema Discovery: %s\n", getStatusText(m.schemaDiscovery.Enabled)) +
				fmt.Sprintf("• Cleanup Rules: %d", len(m.deletionRules)))
		content.WriteString("\n" + summary + "\n")
	}

	// Simple help text
	helpText := helpStyle.Render(
		"\n📋 Navigation:\n" +
			"  ↑/↓ or k/j    Navigate options\n" +
			"  Enter         Configure selected feature\n" +
			"  Esc           Return to main menu\n\n" +
			"💡 Tip: Start with Custom Search Patterns to define what data to look for.")

	content.WriteString(helpText)

	return content.String()
}

func getStatusText(enabled bool) string {
	if enabled {
		return "enabled"
	}
	return "disabled"
}

func renderSchemaConfig(m *model) string {
	var content strings.Builder

	title := titleStyle.Render("🏗️ Schema Discovery")
	content.WriteString(title + "\n\n")

	description := descriptionStyle.Render(
		"Analyze your data structure to understand field types, patterns, and relationships.")
	content.WriteString(description + "\n\n")

	// Simple enable/disable toggle
	enabledText := warningStyle.Render("✗ Disabled")
	if m.schemaDiscovery.Enabled {
		enabledText = successStyle.Render("✓ Enabled")
	}

	statusCard := cardStyle.Render(
		subtitleStyle.Render("Current Status") + "\n" +
			"Schema Discovery: " + enabledText + "\n" +
			fmt.Sprintf("Sample Rate: %.1f%% of data\n", m.schemaDiscovery.SamplePercent*100) +
			fmt.Sprintf("Analysis Depth: %d levels\n", m.schemaDiscovery.MaxDepth) +
			fmt.Sprintf("Output Formats: %s", strings.Join(m.schemaDiscovery.OutputFormats, ", ")))
	content.WriteString(statusCard + "\n\n")

	// Simple menu options
	menuOptions := []struct {
		title       string
		description string
		action      string
	}{
		{
			title:       "Toggle Schema Discovery",
			description: "Enable or disable schema analysis",
			action:      "toggle",
		},
		{
			title:       "Quick Setup (Recommended)",
			description: "Use standard settings for most datasets",
			action:      "quick",
		},
		{
			title:       "Custom Configuration",
			description: "Adjust sample rate, depth, and output formats",
			action:      "custom",
		},
		{
			title:       "← Back to Advanced Menu",
			description: "Return to advanced features",
			action:      "back",
		},
	}

	for i, option := range menuOptions {
		cursor := "  "
		style := lipgloss.NewStyle()

		if m.schemaConfigCursor == i {
			cursor = menuCursorGlyph
			style = menuCursorStyle
		}

		optionLine := style.Render(cursor + option.title)
		descLine := descriptionStyle.Render("    " + option.description)

		content.WriteString(optionLine + "\n")
		content.WriteString(descLine + "\n")

		if i < len(menuOptions)-1 {
			content.WriteString("\n")
		}
	}

	helpText := helpStyle.Render(
		"\n📋 Controls:\n" +
			"  ↑/↓ or k/j    Navigate options\n" +
			"  Enter         Select option\n" +
			"  Esc           Return to advanced menu\n\n" +
			"💡 Tip: Schema discovery helps understand your data structure before analysis.")

	content.WriteString(helpText)

	return content.String()
}

func renderSchemaResult() string {
	return headerStyle.Render("Schema Analysis Results") + "\n\n" +
		"Schema analysis would be performed here with the current configuration.\n" +
		helpStyle.Render("\nPress any key to return to schema configuration.")
}

func renderSearchConfig(m *model) string {
	s := headerStyle.Render("Search Configuration") + "\n\n"

	if len(m.searchTargets) == 0 {
		s += "No search targets configured.\n\n"
	} else {
		s += "Current Search Targets:\n"
		for i, target := range m.searchTargets {
			cursor := " "
			if m.searchConfigCursor == i {
				cursor = ">"
			}
			s += fmt.Sprintf("%s %s (%s: %s)\n",
				menuCursorStyle.Render(cursor),
				target.Name,
				target.Type,
				target.Path)
		}
		s += "\n"
	}

	// Add new target option
	cursor := " "
	if m.searchConfigCursor == len(m.searchTargets) {
		cursor = ">"
	}
	s += fmt.Sprintf("%s Add New Target\n", menuCursorStyle.Render(cursor))

	// Back option
	cursor = " "
	if m.searchConfigCursor == len(m.searchTargets)+1 {
		cursor = ">"
	}
	s += fmt.Sprintf("%s Back to Advanced Menu\n", menuCursorStyle.Render(cursor))

	return s + helpStyle.Render("\nUse up/down arrows, Enter to edit, 'd' to delete, esc to go back.")
}

func renderSearchTargetEdit(m *model) string {
	if m.editingSearchTarget == nil {
		return "No target being edited"
	}

	s := headerStyle.Render("Edit Search Target") + "\n\n"

	fields := []string{
		fmt.Sprintf("Name: %s", m.searchNameInput.View()),
		fmt.Sprintf("Type: %s (Tab to cycle)", m.editingSearchTarget.Type),
		fmt.Sprintf("Path: %s", m.searchPathInput.View()),
		fmt.Sprintf("Case Sensitive: %t (Tab to toggle)", m.editingSearchTarget.CaseSensitive),
		fmt.Sprintf("Values: %s", m.searchValuesInput.View()),
		"Save Target",
	}

	for i, field := range fields {
		cursor := " "
		if m.editingTargetCursor == i {
			cursor = ">"
		}
		s += fmt.Sprintf("%s %s\n", menuCursorStyle.Render(cursor), field)
	}

	return s + helpStyle.Render("\nUse up/down arrows, Enter to save, Tab to cycle options, esc to cancel.")
}

func renderHashingConfig(m *model) string {
	s := headerStyle.Render("Hashing Strategy Configuration") + "\n\n"

	options := []string{
		fmt.Sprintf("Mode: %s", m.hashingStrategy.Mode),
		"",
		"",
		"Back to Advanced Menu",
	}

	switch m.hashingStrategy.Mode {
	case hashModeSelective:
		options[1] = fmt.Sprintf("Include Keys: %s", strings.Join(m.hashingStrategy.IncludeKeys, ", "))
	case hashModeExcludeKeys:
		options[2] = fmt.Sprintf("Exclude Keys: %s", strings.Join(m.hashingStrategy.ExcludeKeys, ", "))
	}

	for i, option := range options {
		if option == "" {
			continue
		}
		cursor := " "
		if m.hashingConfigCursor == i {
			cursor = ">"
		}
		s += fmt.Sprintf("%s %s\n", menuCursorStyle.Render(cursor), option)
	}

	if m.hashKeysInput.Focused() {
		s += "\n" + m.hashKeysInput.View()
	}

	return s + helpStyle.Render("\nUse up/down arrows, Enter to configure, esc to go back.")
}

func renderDeletionConfig(m *model) string {
	s := headerStyle.Render("Deletion Rules Configuration") + "\n\n"

	if len(m.deletionRules) == 0 {
		s += "No deletion rules configured.\n\n"
	} else {
		s += "Current Deletion Rules:\n"
		for i, rule := range m.deletionRules {
			cursor := " "
			if m.deletionConfigCursor == i {
				cursor = ">"
			}
			s += fmt.Sprintf("%s %s -> %s\n",
				menuCursorStyle.Render(cursor),
				rule.SearchTarget,
				rule.Action)
		}
		s += "\n"
	}

	// Add new rule option
	cursor := " "
	if m.deletionConfigCursor == len(m.deletionRules) {
		cursor = ">"
	}
	s += fmt.Sprintf("%s Add New Rule\n", menuCursorStyle.Render(cursor))

	// Back option
	cursor = " "
	if m.deletionConfigCursor == len(m.deletionRules)+1 {
		cursor = ">"
	}
	s += fmt.Sprintf("%s Back to Advanced Menu\n", menuCursorStyle.Render(cursor))

	return s + helpStyle.Render("\nUse up/down arrows, Enter to edit, 'd' to delete, esc to go back.")
}

func renderDeletionRuleEdit(m *model) string {
	if m.editingDeletionRule == nil {
		return "No rule being edited"
	}

	s := headerStyle.Render("Edit Deletion Rule") + "\n\n"

	fields := []string{
		fmt.Sprintf("Search Target: %s (Tab to cycle)", m.editingDeletionRule.SearchTarget),
		fmt.Sprintf("Action: %s (Tab to cycle)", m.editingDeletionRule.Action),
		fmt.Sprintf("Output Path: %s", m.ruleOutputPathInput.View()),
		"Save Rule",
	}

	for i, field := range fields {
		cursor := " "
		if m.editingRuleCursor == i {
			cursor = ">"
		}
		s += fmt.Sprintf("%s %s\n", menuCursorStyle.Render(cursor), field)
	}

	return s + helpStyle.Render("\nUse up/down arrows, Enter to save, Tab to cycle options, esc to cancel.")
}

func renderAdvancedOutput(m *model) string {
	s := headerStyle.Render("Advanced Output Configuration") + "\n\n"

	options := []string{
		fmt.Sprintf("Advanced Features: %t", m.advancedEnabled),
		"Save Configuration",
		"Back to Advanced Menu",
	}

	for i, option := range options {
		cursor := " "
		if m.outputConfigCursor == i {
			cursor = ">"
		}
		s += fmt.Sprintf("%s %s\n", menuCursorStyle.Render(cursor), option)
	}

	summary := "\nCurrent Configuration Summary:\n"
	summary += fmt.Sprintf("- Search Targets: %d\n", len(m.searchTargets))
	summary += fmt.Sprintf("- Deletion Rules: %d\n", len(m.deletionRules))
	summary += fmt.Sprintf("- Hashing Mode: %s\n", m.hashingStrategy.Mode)
	summary += fmt.Sprintf("- Schema Discovery: %t\n", m.schemaDiscovery.Enabled)

	return s + summary + helpStyle.Render("\nUse up/down arrows, Enter to select, esc to go back.")
}
