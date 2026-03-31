// Package rewrite provides streamed JSON/NDJSON rewrite workflows.
package rewrite

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"

	"github.com/benjaminwestern/data-refinery/internal/safety"
	"github.com/benjaminwestern/data-refinery/internal/source"
)

const (
	defaultBufferSize    = 1024 * 1024
	defaultMaxBufferSize = 10 * 1024 * 1024
)

// Mode controls whether rewrite operations are preview-only or applied.
type Mode string

const (
	// ModePreview reports planned changes without writing them.
	ModePreview Mode = "preview"
	// ModeApply writes the rewrite result and creates backups.
	ModeApply Mode = "apply"
)

// Config defines a streamed rewrite workflow for local files or GCS objects.
type Config struct {
	Paths              []string
	Workers            int
	LogPath            string
	ApprovedOutputRoot string
	Mode               Mode
	BackupDir          string
	BufferSize         int
	MaxBufferSize      int
	TopLevelKey        string
	TopLevelValues     []string
	ArrayKey           string
	ArrayDeleteKey     string
	ArrayDeleteValues  []string
	StateKey           string
	StateValue         string
	UpdateKey          string
	UpdateOldValue     string
	UpdateNewValue     string
	UpdateIDKey        string
	UpdateIDValues     []string
	UpdateStateKey     string
	UpdateStateValue   string
}

// Summary captures the outcome of a rewrite run.
type Summary struct {
	Mode            string
	FilesDiscovered int
	FilesProcessed  int
	FilesModified   int
	LinesRead       int64
	LinesDeleted    int64
	LinesModified   int64
	LinesUpdated    int64
	Errors          int64
	BackupPath      string
}

type compiledConfig struct {
	config          *Config
	topLevelValues  map[string]struct{}
	arrayDeleteVals map[string]struct{}
	updateIDValues  map[string]struct{}
	backupPath      string
	bufferSize      int
	maxBufferSize   int
}

type counters struct {
	filesProcessed atomic.Int32
	filesModified  atomic.Int32
	linesRead      atomic.Int64
	linesDeleted   atomic.Int64
	linesModified  atomic.Int64
	linesUpdated   atomic.Int64
	errors         atomic.Int64
}

type lineResult struct {
	Output   []byte
	Keep     bool
	Deleted  bool
	Modified bool
	Updated  bool
}

type rewriteTarget interface {
	WriteLine([]byte) error
	Commit(context.Context) error
	Abort(context.Context) error
}

type localTarget struct {
	src        source.InputSource
	backupPath string
	tempFile   *os.File
	writer     *bufio.Writer
	tempPath   string
}

type gcsTarget struct {
	src        source.InputSource
	backupPath string
	client     *storage.Client
	bucket     string
	objectName string
	sourceGen  int64
	tempObject string
	tempGen    int64
	writer     *storage.Writer
}

// Validate checks whether the rewrite configuration is complete enough to run.
func (c *Config) Validate() error {
	if len(c.Paths) == 0 {
		return fmt.Errorf("at least one -path is required")
	}
	if c.Workers < 1 {
		return fmt.Errorf("workers must be at least 1")
	}
	if c.Mode == "" {
		c.Mode = ModePreview
	}
	if c.Mode != ModePreview && c.Mode != ModeApply {
		return fmt.Errorf("mode must be %q or %q", ModePreview, ModeApply)
	}
	if c.Mode == ModeApply && strings.TrimSpace(c.BackupDir) == "" {
		return fmt.Errorf("backup-dir is required in apply mode")
	}
	if (c.StateKey == "") != (c.StateValue == "") {
		return fmt.Errorf("state-key and state-value must be provided together")
	}
	if (c.TopLevelKey == "") != (len(c.TopLevelValues) == 0) {
		return fmt.Errorf("top-level-key and top-level-vals must be provided together")
	}
	hasArrayDelete := c.ArrayKey != "" || c.ArrayDeleteKey != "" || len(c.ArrayDeleteValues) > 0
	if hasArrayDelete && (c.ArrayKey == "" || c.ArrayDeleteKey == "" || len(c.ArrayDeleteValues) == 0) {
		return fmt.Errorf("array-key, array-del-key, and array-del-vals must be provided together")
	}
	if (c.UpdateStateKey == "") != (c.UpdateStateValue == "") {
		return fmt.Errorf("update-state-key and update-state-value must be provided together")
	}
	if (c.UpdateIDKey == "") != (len(c.UpdateIDValues) == 0) {
		return fmt.Errorf("update-id-key and update-id-vals must be provided together")
	}
	if c.UpdateKey != "" || c.UpdateOldValue != "" || c.UpdateNewValue != "" {
		if c.UpdateKey == "" || c.UpdateOldValue == "" || c.UpdateNewValue == "" {
			return fmt.Errorf("update-key, update-old-value, and update-new-value must be provided together")
		}
	}
	hasDelete := c.TopLevelKey != "" && len(c.TopLevelValues) > 0
	hasArrayDelete = c.ArrayKey != "" && c.ArrayDeleteKey != "" && len(c.ArrayDeleteValues) > 0
	hasUpdate := c.UpdateKey != ""
	if !hasDelete && !hasArrayDelete && !hasUpdate {
		return fmt.Errorf("configure at least one delete or update operation")
	}

	return nil
}

// Run executes the rewrite workflow across every discovered input source.
func Run(ctx context.Context, cfg *Config) (*Summary, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	sources, err := source.DiscoverAll(ctx, cfg.Paths)
	if err != nil {
		return nil, fmt.Errorf("discover rewrite sources: %w", err)
	}

	compiled := compileConfig(cfg)
	runCounters := &counters{}
	sourceCh := make(chan source.InputSource)
	errCh := make(chan error, len(sources))

	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for src := range sourceCh {
				if err := processSource(ctx, compiled, src, runCounters); err != nil {
					errCh <- err
				}
			}
		}()
	}

	go func() {
		defer close(sourceCh)
		for _, src := range sources {
			select {
			case sourceCh <- src:
			case <-ctx.Done():
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)

	var failures []string
	for err := range errCh {
		failures = append(failures, err.Error())
	}
	if ctx.Err() != nil {
		failures = append(failures, ctx.Err().Error())
	}
	if len(failures) > 0 {
		return nil, fmt.Errorf("%s", strings.Join(failures, "; "))
	}

	return &Summary{
		Mode:            string(cfg.Mode),
		FilesDiscovered: len(sources),
		FilesProcessed:  int(runCounters.filesProcessed.Load()),
		FilesModified:   int(runCounters.filesModified.Load()),
		LinesRead:       runCounters.linesRead.Load(),
		LinesDeleted:    runCounters.linesDeleted.Load(),
		LinesModified:   runCounters.linesModified.Load(),
		LinesUpdated:    runCounters.linesUpdated.Load(),
		Errors:          runCounters.errors.Load(),
		BackupPath:      compiled.backupPath,
	}, nil
}

func compileConfig(cfg *Config) *compiledConfig {
	bufferSize := cfg.BufferSize
	if bufferSize <= 0 {
		bufferSize = defaultBufferSize
	}
	maxBufferSize := cfg.MaxBufferSize
	if maxBufferSize <= 0 {
		maxBufferSize = defaultMaxBufferSize
	}
	if maxBufferSize < bufferSize {
		maxBufferSize = bufferSize
	}

	compiled := &compiledConfig{
		config:          cfg,
		topLevelValues:  makeStringSet(cfg.TopLevelValues),
		arrayDeleteVals: makeStringSet(cfg.ArrayDeleteValues),
		updateIDValues:  makeStringSet(cfg.UpdateIDValues),
		bufferSize:      bufferSize,
		maxBufferSize:   maxBufferSize,
	}

	if cfg.Mode == ModeApply {
		compiled.backupPath = filepath.Join(
			cfg.BackupDir,
			time.Now().Format("2006-01-02_15-04-05"),
		)
	}

	return compiled
}

func processSource(ctx context.Context, cfg *compiledConfig, src source.InputSource, counters *counters) error {
	reader, err := src.Open(ctx)
	if err != nil {
		return fmt.Errorf("open %s: %w", src.Path(), err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.Printf("failed to close source %s: %v", src.Path(), closeErr)
		}
	}()

	var target rewriteTarget
	if cfg.config.Mode == ModeApply {
		target, err = newRewriteTarget(ctx, src, cfg.backupPath)
		if err != nil {
			return fmt.Errorf("prepare target for %s: %w", src.Path(), err)
		}
		defer func() {
			if target != nil {
				if abortErr := target.Abort(ctx); abortErr != nil {
					log.Printf("failed to abort rewrite target for %s: %v", src.Path(), abortErr)
				}
			}
		}()
	}

	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, cfg.bufferSize), cfg.maxBufferSize)

	fileModified := false

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return fmt.Errorf("rewrite canceled for %s: %w", src.Path(), ctx.Err())
		default:
		}

		line := append([]byte(nil), scanner.Bytes()...)
		counters.linesRead.Add(1)

		result, lineErr := processLine(line, cfg)
		if lineErr != nil {
			counters.errors.Add(1)
		}

		if result.Deleted {
			counters.linesDeleted.Add(1)
		}
		if result.Modified {
			counters.linesModified.Add(1)
		}
		if result.Updated {
			counters.linesUpdated.Add(1)
		}
		if result.Deleted || result.Modified || result.Updated {
			fileModified = true
		}

		if target != nil && result.Keep {
			if err := target.WriteLine(result.Output); err != nil {
				return fmt.Errorf("write rewritten line for %s: %w", src.Path(), err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan %s: %w", src.Path(), err)
	}

	if err := finalizeRewriteTarget(ctx, target, src.Path(), fileModified); err != nil {
		return err
	}
	target = nil

	counters.filesProcessed.Add(1)
	if fileModified {
		counters.filesModified.Add(1)
	}

	return nil
}

func processLine(line []byte, cfg *compiledConfig) (lineResult, error) {
	if len(line) == 0 {
		return lineResult{Output: line, Keep: true}, nil
	}

	var obj map[string]any
	if err := json.Unmarshal(line, &obj); err != nil {
		return lineResult{Output: line, Keep: true}, fmt.Errorf("decode rewrite line: %w", err)
	}

	if shouldMatchDelete(obj, cfg.config.TopLevelKey, cfg.topLevelValues, cfg.config.StateKey, cfg.config.StateValue) {
		return lineResult{Keep: false, Deleted: true}, nil
	}

	modified := applyArrayDeletion(obj, cfg)

	updated := false
	if cfg.config.UpdateKey != "" && shouldApplyUpdate(obj, cfg) {
		updated = updateInObject(obj, cfg.config.UpdateKey, cfg.config.UpdateOldValue, cfg.config.UpdateNewValue)
	}

	if !modified && !updated {
		return lineResult{Output: line, Keep: true}, nil
	}

	output, err := json.Marshal(obj)
	if err != nil {
		return lineResult{Output: line, Keep: true}, fmt.Errorf("encode rewritten line: %w", err)
	}

	return lineResult{
		Output:   output,
		Keep:     true,
		Modified: modified,
		Updated:  updated,
	}, nil
}

func finalizeRewriteTarget(ctx context.Context, target rewriteTarget, srcPath string, fileModified bool) error {
	if target == nil {
		return nil
	}

	if fileModified {
		if err := target.Commit(ctx); err != nil {
			return fmt.Errorf("commit rewrite for %s: %w", srcPath, err)
		}
		return nil
	}

	if err := target.Abort(ctx); err != nil {
		return fmt.Errorf("discard rewrite for %s: %w", srcPath, err)
	}

	return nil
}

func applyArrayDeletion(obj map[string]any, cfg *compiledConfig) bool {
	if cfg.config.ArrayKey == "" || cfg.config.ArrayDeleteKey == "" || len(cfg.arrayDeleteVals) == 0 {
		return false
	}

	arrayVal, exists := obj[cfg.config.ArrayKey]
	if !exists {
		return false
	}

	array, ok := arrayVal.([]any)
	if !ok {
		return false
	}

	filtered, modified := filterArrayItems(
		array,
		cfg.config.ArrayDeleteKey,
		cfg.arrayDeleteVals,
		cfg.config.StateKey,
		cfg.config.StateValue,
	)
	if modified {
		obj[cfg.config.ArrayKey] = filtered
	}

	return modified
}

func filterArrayItems(array []any, deleteKey string, deleteValues map[string]struct{}, stateKey, stateValue string) ([]any, bool) {
	filtered := make([]any, 0, len(array))
	modified := false

	for _, item := range array {
		itemMap, ok := item.(map[string]any)
		if !ok {
			filtered = append(filtered, item)
			continue
		}
		if shouldMatchDelete(itemMap, deleteKey, deleteValues, stateKey, stateValue) {
			modified = true
			continue
		}
		filtered = append(filtered, item)
	}

	return filtered, modified
}

func shouldMatchDelete(item map[string]any, key string, values map[string]struct{}, stateKey, stateValue string) bool {
	if key == "" || len(values) == 0 {
		return false
	}

	value, exists := item[key]
	if !exists || !containsValue(values, value) {
		return false
	}

	if stateKey != "" && stateValue != "" {
		state, exists := item[stateKey]
		return exists && fmt.Sprintf("%v", state) == stateValue
	}

	return true
}

func shouldApplyUpdate(item map[string]any, cfg *compiledConfig) bool {
	if cfg.config.UpdateIDKey != "" && len(cfg.updateIDValues) > 0 {
		value, exists := item[cfg.config.UpdateIDKey]
		if !exists || !containsValue(cfg.updateIDValues, value) {
			return false
		}
	}

	if cfg.config.UpdateStateKey != "" && cfg.config.UpdateStateValue != "" {
		value, exists := item[cfg.config.UpdateStateKey]
		if !exists || fmt.Sprintf("%v", value) != cfg.config.UpdateStateValue {
			return false
		}
	}

	return true
}

func updateInObject(obj map[string]any, updateKey, oldValue, newValue string) bool {
	updated := false

	for key, value := range obj {
		if key == updateKey && fmt.Sprintf("%v", value) == oldValue {
			obj[key] = convertValue(newValue, value)
			updated = true
			continue
		}

		nestedObj, ok := value.(map[string]any)
		if ok {
			updated = updateInObject(nestedObj, updateKey, oldValue, newValue) || updated
			continue
		}

		array, ok := value.([]any)
		if !ok {
			continue
		}

		for _, item := range array {
			itemMap, ok := item.(map[string]any)
			if !ok {
				continue
			}
			updated = updateInObject(itemMap, updateKey, oldValue, newValue) || updated
		}
	}

	return updated
}

func convertValue(newValue string, original any) any {
	switch original.(type) {
	case bool:
		if parsed, err := strconv.ParseBool(newValue); err == nil {
			return parsed
		}
		if newValue == "0" {
			return false
		}
		if newValue == "1" {
			return true
		}
	case float64:
		if parsed, err := strconv.ParseFloat(newValue, 64); err == nil {
			return parsed
		}
	case int:
		if parsed, err := strconv.Atoi(newValue); err == nil {
			return parsed
		}
	}

	return newValue
}

// ParseValuesInput expands a comma-separated string or single-column CSV file
// into a list of string values.
func ParseValuesInput(input string) ([]string, error) {
	if input == "" {
		return nil, nil
	}
	if strings.HasSuffix(strings.ToLower(input), ".csv") {
		return parseCSVFile(input)
	}
	return parseCommaSeparatedValues(input), nil
}

func parseCSVFile(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open csv file: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Printf("failed to close csv file %s: %v", filePath, closeErr)
		}
	}()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1

	var values []string
	lineNumber := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read csv line %d: %w", lineNumber+1, err)
		}

		lineNumber++
		if lineNumber == 1 {
			continue
		}
		if len(record) != 1 {
			return nil, fmt.Errorf("line %d: expected a single column", lineNumber)
		}

		value := strings.TrimSpace(record[0])
		if value != "" {
			values = append(values, value)
		}
	}

	if lineNumber == 0 {
		return nil, fmt.Errorf("csv file is empty")
	}
	if lineNumber == 1 {
		return nil, fmt.Errorf("csv file contains only a header row")
	}

	return values, nil
}

func parseCommaSeparatedValues(input string) []string {
	parts := strings.Split(input, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value != "" {
			values = append(values, value)
		}
	}
	return values
}

func makeStringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func containsValue(values map[string]struct{}, candidate any) bool {
	_, exists := values[fmt.Sprintf("%v", candidate)]
	return exists
}

func newRewriteTarget(ctx context.Context, src source.InputSource, backupPath string) (rewriteTarget, error) {
	if strings.HasPrefix(src.Path(), "gs://") {
		return newGCSTarget(ctx, src, backupPath)
	}
	return newLocalTarget(src, backupPath)
}

func newLocalTarget(src source.InputSource, backupPath string) (*localTarget, error) {
	dir := filepath.Dir(src.Path())
	tempFile, err := os.CreateTemp(dir, filepath.Base(src.Path())+".data-refinery-*")
	if err != nil {
		return nil, fmt.Errorf("create temp file for %s: %w", src.Path(), err)
	}
	if err := tempFile.Chmod(0o600); err != nil {
		safety.Close(tempFile, src.Path())
		return nil, fmt.Errorf("set temp file permissions for %s: %w", src.Path(), err)
	}

	return &localTarget{
		src:        src,
		backupPath: backupPath,
		tempFile:   tempFile,
		writer:     bufio.NewWriter(tempFile),
		tempPath:   tempFile.Name(),
	}, nil
}

func (t *localTarget) WriteLine(line []byte) error {
	if _, err := t.writer.Write(line); err != nil {
		return fmt.Errorf("write temp data for %s: %w", t.src.Path(), err)
	}
	if err := t.writer.WriteByte('\n'); err != nil {
		return fmt.Errorf("write temp newline for %s: %w", t.src.Path(), err)
	}
	return nil
}

func (t *localTarget) Commit(ctx context.Context) error {
	if err := t.writer.Flush(); err != nil {
		return fmt.Errorf("flush temp file for %s: %w", t.src.Path(), err)
	}
	if err := t.tempFile.Close(); err != nil {
		return fmt.Errorf("close temp file for %s: %w", t.src.Path(), err)
	}

	if err := backupSource(ctx, t.src, t.backupPath); err != nil {
		return fmt.Errorf("backup source %s: %w", t.src.Path(), err)
	}

	if info, err := os.Stat(t.src.Path()); err == nil {
		if chmodErr := os.Chmod(t.tempPath, info.Mode()); chmodErr != nil {
			return fmt.Errorf("preserve file mode for %s: %w", t.src.Path(), chmodErr)
		}
	}

	if err := os.Rename(t.tempPath, t.src.Path()); err != nil {
		return fmt.Errorf("replace source file %s: %w", t.src.Path(), err)
	}

	return nil
}

func (t *localTarget) Abort(_ context.Context) error {
	flushErr := t.writer.Flush()
	closeErr := t.tempFile.Close()
	removeErr := os.Remove(t.tempPath)

	if flushErr != nil {
		return fmt.Errorf("flush temp file for %s: %w", t.src.Path(), flushErr)
	}
	if closeErr != nil && !strings.Contains(closeErr.Error(), "file already closed") {
		return fmt.Errorf("close temp file for %s: %w", t.src.Path(), closeErr)
	}
	if removeErr != nil && !os.IsNotExist(removeErr) {
		return fmt.Errorf("remove temp file %s: %w", t.tempPath, removeErr)
	}

	return nil
}

func newGCSTarget(ctx context.Context, src source.InputSource, backupPath string) (*gcsTarget, error) {
	bucket, objectName, err := parseGCSPath(src.Path())
	if err != nil {
		return nil, err
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create GCS client for %s: %w", src.Path(), err)
	}

	generationSource, ok := src.(interface{ Generation() int64 })
	if !ok || generationSource.Generation() == 0 {
		safety.Close(client, src.Path())
		return nil, fmt.Errorf("source %s is missing generation metadata required for safe GCS rewrites", src.Path())
	}

	tempObject := fmt.Sprintf("%s.data-refinery.%d.tmp", objectName, time.Now().UnixNano())
	writer := client.Bucket(bucket).Object(tempObject).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	writer.ContentType = detectContentType(src.Path())

	return &gcsTarget{
		src:        src,
		backupPath: backupPath,
		client:     client,
		bucket:     bucket,
		objectName: objectName,
		sourceGen:  generationSource.Generation(),
		tempObject: tempObject,
		writer:     writer,
	}, nil
}

func (t *gcsTarget) WriteLine(line []byte) error {
	if _, err := t.writer.Write(line); err != nil {
		return fmt.Errorf("write temp GCS data for %s: %w", t.src.Path(), err)
	}
	_, err := t.writer.Write([]byte("\n"))
	if err != nil {
		return fmt.Errorf("write temp GCS newline for %s: %w", t.src.Path(), err)
	}

	return nil
}

func (t *gcsTarget) Commit(ctx context.Context) error {
	if err := t.writer.Close(); err != nil {
		return fmt.Errorf("close temp GCS writer for %s: %w", t.src.Path(), err)
	}
	tempAttrs := t.writer.Attrs()
	if tempAttrs == nil || tempAttrs.Generation == 0 {
		return fmt.Errorf("temp GCS object for %s is missing generation metadata", t.src.Path())
	}
	t.tempGen = tempAttrs.Generation
	t.writer = nil

	if err := backupSource(ctx, t.src, t.backupPath); err != nil {
		return fmt.Errorf("backup source %s: %w", t.src.Path(), err)
	}

	srcObj := t.client.Bucket(t.bucket).Object(t.tempObject).If(storage.Conditions{GenerationMatch: t.tempGen})
	dstObj := t.client.Bucket(t.bucket).Object(t.objectName).If(storage.Conditions{GenerationMatch: t.sourceGen})
	if _, err := dstObj.CopierFrom(srcObj).Run(ctx); err != nil {
		return fmt.Errorf("promote temp GCS object for %s: %w", t.src.Path(), err)
	}
	if err := srcObj.Delete(ctx); err != nil {
		return fmt.Errorf("delete temp GCS object for %s: %w", t.src.Path(), err)
	}

	if err := t.client.Close(); err != nil {
		return fmt.Errorf("close GCS client for %s: %w", t.src.Path(), err)
	}

	return nil
}

func (t *gcsTarget) Abort(ctx context.Context) error {
	var firstErr error

	if t.writer != nil {
		if err := t.writer.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close temp GCS writer for %s: %w", t.src.Path(), err)
		}
		t.writer = nil
	}

	if err := t.client.Bucket(t.bucket).Object(t.tempObject).Delete(ctx); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("delete temp GCS object for %s: %w", t.src.Path(), err)
	}
	if err := t.client.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("close GCS client for %s: %w", t.src.Path(), err)
	}

	return firstErr
}

func backupSource(ctx context.Context, src source.InputSource, backupRoot string) error {
	if backupRoot == "" {
		return nil
	}

	reader, err := src.Open(ctx)
	if err != nil {
		return fmt.Errorf("re-open source for backup: %w", err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.Printf("failed to close backup source %s: %v", src.Path(), closeErr)
		}
	}()

	backupPath, err := buildBackupPath(backupRoot, src.Path())
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(backupPath), 0o700); err != nil {
		return fmt.Errorf("create backup directory for %s: %w", backupPath, err)
	}

	file, err := os.OpenFile(backupPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create backup file %s: %w", backupPath, err)
	}
	if err := file.Chmod(0o600); err != nil {
		safety.Close(file, backupPath)
		return fmt.Errorf("set backup file permissions for %s: %w", backupPath, err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			log.Printf("failed to close backup file %s: %v", backupPath, closeErr)
		}
	}()

	_, err = io.Copy(file, reader)
	if err != nil {
		return fmt.Errorf("copy source to backup %s: %w", backupPath, err)
	}

	return nil
}

func buildBackupPath(root, original string) (string, error) {
	if strings.HasPrefix(original, "gs://") {
		bucket, objectName, err := parseGCSPath(original)
		if err != nil {
			return "", err
		}
		path, err := source.BuildContainedGCSLocalPath(root, bucket, objectName)
		if err != nil {
			return "", fmt.Errorf("build contained GCS backup path: %w", err)
		}
		return path, nil
	}

	path, err := source.BuildContainedLocalPath(root, original)
	if err != nil {
		return "", fmt.Errorf("build contained local backup path: %w", err)
	}
	return path, nil
}

func parseGCSPath(gcsPath string) (string, string, error) {
	trimmed := strings.TrimPrefix(gcsPath, "gs://")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid GCS path %q", gcsPath)
	}
	return parts[0], parts[1], nil
}

func detectContentType(path string) string {
	lower := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lower, ".ndjson"), strings.HasSuffix(lower, ".jsonl"):
		return "application/x-ndjson"
	case strings.HasSuffix(lower, ".json"):
		return "application/json"
	default:
		return "application/octet-stream"
	}
}
