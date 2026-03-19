package performance

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/benjaminwestern/data-refinery/internal/tui/layout"
	"github.com/benjaminwestern/data-refinery/internal/tui/theme"
)

// RenderCache manages cached rendered content for performance
type RenderCache struct {
	cache map[string]CachedItem
	mutex sync.RWMutex
	ttl   time.Duration
}

// CachedItem represents a cached rendered item
type CachedItem struct {
	Content   string
	Timestamp time.Time
	Hash      string
}

// NewRenderCache creates a new render cache
func NewRenderCache(ttl time.Duration) *RenderCache {
	return &RenderCache{
		cache: make(map[string]CachedItem),
		ttl:   ttl,
	}
}

// Get retrieves a cached item if it exists and is still valid
func (rc *RenderCache) Get(key string) (string, bool) {
	rc.mutex.RLock()
	defer rc.mutex.RUnlock()

	item, exists := rc.cache[key]
	if !exists {
		return "", false
	}

	// Check if item has expired
	if time.Since(item.Timestamp) > rc.ttl {
		return "", false
	}

	return item.Content, true
}

// Set stores a rendered item in the cache
func (rc *RenderCache) Set(key, content, hash string) {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()

	rc.cache[key] = CachedItem{
		Content:   content,
		Timestamp: time.Now(),
		Hash:      hash,
	}
}

// Clear removes all cached items
func (rc *RenderCache) Clear() {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()

	rc.cache = make(map[string]CachedItem)
}

// CleanExpired removes expired items from the cache
func (rc *RenderCache) CleanExpired() {
	rc.mutex.Lock()
	defer rc.mutex.Unlock()

	now := time.Now()
	for key, item := range rc.cache {
		if now.Sub(item.Timestamp) > rc.ttl {
			delete(rc.cache, key)
		}
	}
}

// VirtualScrolling manages efficient rendering of large lists
type VirtualScrolling struct {
	items        []VirtualItem
	visibleStart int
	visibleEnd   int
	viewportSize int
	itemHeight   int
	totalHeight  int
	scrollOffset int
}

// VirtualItem represents an item in the virtual scroll
type VirtualItem struct {
	Content    string
	Height     int
	Selected   bool
	Cached     bool
	LastRender time.Time
}

// NewVirtualScrolling creates a new virtual scrolling manager
func NewVirtualScrolling(viewportSize, itemHeight int) *VirtualScrolling {
	return &VirtualScrolling{
		items:        []VirtualItem{},
		viewportSize: viewportSize,
		itemHeight:   itemHeight,
		visibleStart: 0,
		visibleEnd:   0,
	}
}

// SetItems sets the items for virtual scrolling
func (vs *VirtualScrolling) SetItems(items []string) {
	vs.items = make([]VirtualItem, len(items))
	for i, item := range items {
		vs.items[i] = VirtualItem{
			Content:    item,
			Height:     vs.itemHeight,
			Selected:   false,
			Cached:     false,
			LastRender: time.Time{},
		}
	}
	vs.totalHeight = len(items) * vs.itemHeight
	vs.updateVisibleRange()
}

// ScrollTo scrolls to a specific item index
func (vs *VirtualScrolling) ScrollTo(index int) {
	if index < 0 || index >= len(vs.items) {
		return
	}

	vs.scrollOffset = index * vs.itemHeight
	vs.updateVisibleRange()
}

// ScrollBy scrolls by a specific amount
func (vs *VirtualScrolling) ScrollBy(delta int) {
	vs.scrollOffset += delta

	// Clamp scroll offset
	maxScroll := vs.totalHeight - vs.viewportSize
	if vs.scrollOffset < 0 {
		vs.scrollOffset = 0
	} else if vs.scrollOffset > maxScroll {
		vs.scrollOffset = maxScroll
	}

	vs.updateVisibleRange()
}

// updateVisibleRange calculates which items are visible
func (vs *VirtualScrolling) updateVisibleRange() {
	vs.visibleStart = vs.scrollOffset / vs.itemHeight
	vs.visibleEnd = vs.visibleStart + (vs.viewportSize / vs.itemHeight) + 1

	// Clamp to valid range
	if vs.visibleStart < 0 {
		vs.visibleStart = 0
	}
	if vs.visibleEnd > len(vs.items) {
		vs.visibleEnd = len(vs.items)
	}
}

// GetVisibleItems returns the currently visible items
func (vs *VirtualScrolling) GetVisibleItems() []VirtualItem {
	if vs.visibleStart >= len(vs.items) {
		return []VirtualItem{}
	}

	return vs.items[vs.visibleStart:vs.visibleEnd]
}

// GetVisibleRange returns the start and end indices of visible items
func (vs *VirtualScrolling) GetVisibleRange() (int, int) {
	return vs.visibleStart, vs.visibleEnd
}

// SetSelected sets the selected state of an item
func (vs *VirtualScrolling) SetSelected(index int, selected bool) {
	if index >= 0 && index < len(vs.items) {
		vs.items[index].Selected = selected
	}
}

// PerformanceOptimizer manages overall TUI rendering performance
type PerformanceOptimizer struct {
	renderCache      *RenderCache
	virtualScrolling *VirtualScrolling
	frameRateLimit   time.Duration
	lastRender       time.Time
	renderStats      RenderStats
	diffRenderer     *DiffRenderer
	layoutCache      *LayoutCache
}

// RenderStats tracks rendering performance metrics
type RenderStats struct {
	TotalRenders      int64
	CacheHits         int64
	CacheMisses       int64
	AverageRenderTime time.Duration
	LastRenderTime    time.Duration
	FrameRate         float64
}

// NewPerformanceOptimizer creates a new performance optimizer
func NewPerformanceOptimizer(frameRateLimit time.Duration) *PerformanceOptimizer {
	return &PerformanceOptimizer{
		renderCache:      NewRenderCache(5 * time.Second),
		virtualScrolling: NewVirtualScrolling(50, 1),
		frameRateLimit:   frameRateLimit,
		lastRender:       time.Time{},
		renderStats:      RenderStats{},
		diffRenderer:     NewDiffRenderer(),
		layoutCache:      NewLayoutCache(),
	}
}

// ShouldRender checks if enough time has passed to render a new frame
func (po *PerformanceOptimizer) ShouldRender() bool {
	return time.Since(po.lastRender) >= po.frameRateLimit
}

// BeginRender marks the start of a render cycle
func (po *PerformanceOptimizer) BeginRender() time.Time {
	po.lastRender = time.Now()
	return po.lastRender
}

// EndRender marks the end of a render cycle and updates stats
func (po *PerformanceOptimizer) EndRender(startTime time.Time) {
	renderTime := time.Since(startTime)
	po.renderStats.TotalRenders++
	po.renderStats.LastRenderTime = renderTime

	// Update average render time
	if po.renderStats.TotalRenders == 1 {
		po.renderStats.AverageRenderTime = renderTime
	} else {
		po.renderStats.AverageRenderTime = (po.renderStats.AverageRenderTime + renderTime) / 2
	}

	// Update frame rate
	if renderTime > 0 {
		po.renderStats.FrameRate = 1.0 / renderTime.Seconds()
	}
}

// GetRenderStats returns current rendering statistics
func (po *PerformanceOptimizer) GetRenderStats() RenderStats {
	return po.renderStats
}

// OptimizeContent optimizes content rendering using various techniques
func (po *PerformanceOptimizer) OptimizeContent(content string, context RenderContext) string {
	startTime := time.Now()
	defer po.EndRender(startTime)

	// Check cache first
	cacheKey := po.generateCacheKey(content, context)
	if cached, hit := po.renderCache.Get(cacheKey); hit {
		po.renderStats.CacheHits++
		return cached
	}

	po.renderStats.CacheMisses++

	// Apply optimizations
	optimized := po.applyOptimizations(content, context)

	// Cache the result
	po.renderCache.Set(cacheKey, optimized, po.generateContentHash(content))

	return optimized
}

// RenderContext provides context for rendering optimizations
type RenderContext struct {
	Width       int
	Height      int
	ScrollPos   int
	Theme       *theme.Theme
	Layout      *layout.ResponsiveLayout
	IsVisible   bool
	IsAnimating bool
	Priority    RenderPriority
}

// RenderPriority defines rendering priority levels
type RenderPriority int

const (
	PriorityLow RenderPriority = iota
	PriorityNormal
	PriorityHigh
	PriorityCritical
)

// applyOptimizations applies various rendering optimizations
func (po *PerformanceOptimizer) applyOptimizations(content string, context RenderContext) string {
	// Skip expensive operations for low priority content
	if context.Priority == PriorityLow && len(content) > 10000 {
		return po.simplifyContent(content, context)
	}

	// Use diff rendering for partial updates
	if po.diffRenderer.HasPrevious() {
		return po.diffRenderer.RenderDiff(content, context)
	}

	// Apply layout caching
	if context.Layout != nil {
		if cachedLayout := po.layoutCache.Get(context.Width, context.Height); cachedLayout != nil {
			return po.applyLayoutToContent(content, cachedLayout)
		}
	}

	// For large content, use virtual scrolling
	if len(content) > 5000 && context.Height > 0 {
		return po.virtualRenderContent(content, context)
	}

	return content
}

// simplifyContent creates a simplified version of content for performance
func (po *PerformanceOptimizer) simplifyContent(content string, context RenderContext) string {
	lines := strings.Split(content, "\n")

	// Limit number of lines for very large content
	maxLines := context.Height * 2
	if len(lines) > maxLines {
		visibleLines := lines[:maxLines]
		truncated := append(visibleLines, fmt.Sprintf("... (%d more lines)", len(lines)-maxLines))
		return strings.Join(truncated, "\n")
	}

	return content
}

// virtualRenderContent renders content using virtual scrolling
func (po *PerformanceOptimizer) virtualRenderContent(content string, context RenderContext) string {
	lines := strings.Split(content, "\n")

	// Calculate visible range
	startLine := context.ScrollPos
	endLine := startLine + context.Height

	if startLine < 0 {
		startLine = 0
	}
	if endLine > len(lines) {
		endLine = len(lines)
	}

	// Only render visible lines
	if startLine < len(lines) {
		visibleLines := lines[startLine:endLine]
		return strings.Join(visibleLines, "\n")
	}

	return ""
}

// generateCacheKey generates a cache key for content
func (po *PerformanceOptimizer) generateCacheKey(content string, context RenderContext) string {
	return fmt.Sprintf("%s_%d_%d_%d_%d",
		po.generateContentHash(content),
		context.Width,
		context.Height,
		context.ScrollPos,
		int(context.Priority))
}

// generateContentHash generates a hash of the content
func (po *PerformanceOptimizer) generateContentHash(content string) string {
	// Simple hash based on content length and first/last characters
	if len(content) == 0 {
		return "empty"
	}

	hash := fmt.Sprintf("%d_%c_%c", len(content), content[0], content[len(content)-1])
	return hash
}

// DiffRenderer handles efficient partial updates
type DiffRenderer struct {
	previous    string
	lastRender  time.Time
	diffHistory []string
}

// NewDiffRenderer creates a new diff renderer
func NewDiffRenderer() *DiffRenderer {
	return &DiffRenderer{
		previous:    "",
		lastRender:  time.Time{},
		diffHistory: []string{},
	}
}

// HasPrevious returns true if there's previous content to diff against
func (dr *DiffRenderer) HasPrevious() bool {
	return dr.previous != ""
}

// RenderDiff renders only the differences from previous content
func (dr *DiffRenderer) RenderDiff(content string, context RenderContext) string {
	if dr.previous == content {
		// No changes, return cached result
		return content
	}

	// For now, we'll do a simple line-by-line diff
	prevLines := strings.Split(dr.previous, "\n")
	currLines := strings.Split(content, "\n")

	// Find common prefix and suffix
	prefix := findCommonPrefix(prevLines, currLines)
	suffix := findCommonSuffix(prevLines, currLines)

	// If most of the content is the same, we can optimize
	if prefix+suffix > len(currLines)/2 {
		// Only render the changed middle section
		start := prefix
		end := len(currLines) - suffix
		if start < end {
			changedLines := currLines[start:end]
			result := strings.Join(changedLines, "\n")
			dr.previous = content
			return result
		}
	}

	// Fall back to full render
	dr.previous = content
	return content
}

// findCommonPrefix finds the number of common lines at the start
func findCommonPrefix(a, b []string) int {
	common := 0
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		if a[i] == b[i] {
			common++
		} else {
			break
		}
	}

	return common
}

// findCommonSuffix finds the number of common lines at the end
func findCommonSuffix(a, b []string) int {
	common := 0
	aLen := len(a)
	bLen := len(b)
	minLen := aLen
	if bLen < minLen {
		minLen = bLen
	}

	for i := 1; i <= minLen; i++ {
		if a[aLen-i] == b[bLen-i] {
			common++
		} else {
			break
		}
	}

	return common
}

// LayoutCache caches layout calculations
type LayoutCache struct {
	cache map[string]*layout.ResponsiveLayout
	mutex sync.RWMutex
}

// NewLayoutCache creates a new layout cache
func NewLayoutCache() *LayoutCache {
	return &LayoutCache{
		cache: make(map[string]*layout.ResponsiveLayout),
	}
}

// Get retrieves a cached layout
func (lc *LayoutCache) Get(width, height int) *layout.ResponsiveLayout {
	lc.mutex.RLock()
	defer lc.mutex.RUnlock()

	key := fmt.Sprintf("%d_%d", width, height)
	return lc.cache[key]
}

// Set caches a layout
func (lc *LayoutCache) Set(width, height int, layout *layout.ResponsiveLayout) {
	lc.mutex.Lock()
	defer lc.mutex.Unlock()

	key := fmt.Sprintf("%d_%d", width, height)
	lc.cache[key] = layout
}

// applyLayoutToContent applies cached layout to content
func (po *PerformanceOptimizer) applyLayoutToContent(content string, layout *layout.ResponsiveLayout) string {
	// Use the cached layout to wrap content efficiently
	return layout.WrapContent(content)
}

// BatchRenderer handles efficient batch rendering operations
type BatchRenderer struct {
	batches []RenderBatch
	mutex   sync.Mutex
}

// RenderBatch represents a batch of rendering operations
type RenderBatch struct {
	Items    []BatchItem
	Priority RenderPriority
	Callback func([]string)
}

// BatchItem represents an item in a render batch
type BatchItem struct {
	Content string
	Style   lipgloss.Style
	Context RenderContext
}

// NewBatchRenderer creates a new batch renderer
func NewBatchRenderer() *BatchRenderer {
	return &BatchRenderer{
		batches: []RenderBatch{},
	}
}

// AddBatch adds a rendering batch
func (br *BatchRenderer) AddBatch(batch RenderBatch) {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	br.batches = append(br.batches, batch)
}

// ProcessBatches processes all pending batches
func (br *BatchRenderer) ProcessBatches() {
	br.mutex.Lock()
	defer br.mutex.Unlock()

	for _, batch := range br.batches {
		results := make([]string, len(batch.Items))
		for i, item := range batch.Items {
			results[i] = item.Style.Render(item.Content)
		}

		if batch.Callback != nil {
			batch.Callback(results)
		}
	}

	// Clear processed batches
	br.batches = []RenderBatch{}
}

// PerformanceMonitor monitors TUI performance metrics
type PerformanceMonitor struct {
	startTime   time.Time
	renderTimes []time.Duration
	memoryUsage []int64
	frameRates  []float64
	mutex       sync.RWMutex
	maxSamples  int
}

// NewPerformanceMonitor creates a new performance monitor
func NewPerformanceMonitor(maxSamples int) *PerformanceMonitor {
	return &PerformanceMonitor{
		startTime:   time.Now(),
		renderTimes: []time.Duration{},
		memoryUsage: []int64{},
		frameRates:  []float64{},
		maxSamples:  maxSamples,
	}
}

// RecordRenderTime records a render time sample
func (pm *PerformanceMonitor) RecordRenderTime(duration time.Duration) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.renderTimes = append(pm.renderTimes, duration)

	// Keep only the last N samples
	if len(pm.renderTimes) > pm.maxSamples {
		pm.renderTimes = pm.renderTimes[1:]
	}
}

// RecordFrameRate records a frame rate sample
func (pm *PerformanceMonitor) RecordFrameRate(fps float64) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.frameRates = append(pm.frameRates, fps)

	// Keep only the last N samples
	if len(pm.frameRates) > pm.maxSamples {
		pm.frameRates = pm.frameRates[1:]
	}
}

// GetAverageRenderTime returns the average render time
func (pm *PerformanceMonitor) GetAverageRenderTime() time.Duration {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	if len(pm.renderTimes) == 0 {
		return 0
	}

	total := time.Duration(0)
	for _, rt := range pm.renderTimes {
		total += rt
	}

	return total / time.Duration(len(pm.renderTimes))
}

// GetAverageFrameRate returns the average frame rate
func (pm *PerformanceMonitor) GetAverageFrameRate() float64 {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()

	if len(pm.frameRates) == 0 {
		return 0
	}

	total := 0.0
	for _, fr := range pm.frameRates {
		total += fr
	}

	return total / float64(len(pm.frameRates))
}

// GetPerformanceReport returns a formatted performance report
func (pm *PerformanceMonitor) GetPerformanceReport() string {
	avgRender := pm.GetAverageRenderTime()
	avgFPS := pm.GetAverageFrameRate()
	uptime := time.Since(pm.startTime)

	return fmt.Sprintf(
		"Performance Report:\n"+
			"Uptime: %v\n"+
			"Average Render Time: %v\n"+
			"Average Frame Rate: %.2f FPS\n"+
			"Total Samples: %d",
		uptime,
		avgRender,
		avgFPS,
		len(pm.renderTimes))
}
