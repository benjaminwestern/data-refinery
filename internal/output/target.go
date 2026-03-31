package output

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/benjaminwestern/data-refinery/internal/safety"
)

const defaultTargetBufferSize = 1024 * 1024

// Target is a staged writer that supports plain writes, line writes, and
// commit/abort finalisation for both local paths and GCS URIs.
type Target interface {
	io.Writer
	WriteLine([]byte) error
	Commit(context.Context) error
	Abort(context.Context) error
}

type targetCommitFunc func(context.Context, string, string) error

type targetConfig struct {
	tempDir    string
	bufferSize int
}

// TargetOption configures a Target created by NewTarget.
type TargetOption func(*targetConfig)

// WithTempDir overrides the directory used for the staged temporary file.
func WithTempDir(dir string) TargetOption {
	return func(cfg *targetConfig) {
		cfg.tempDir = strings.TrimSpace(dir)
	}
}

// WithBufferSize overrides the in-memory buffering used for staged writes.
func WithBufferSize(size int) TargetOption {
	return func(cfg *targetConfig) {
		cfg.bufferSize = size
	}
}

// NewTarget creates a staged writer for either a local path or a GCS URI.
func NewTarget(ctx context.Context, targetPath string, opts ...TargetOption) (Target, error) {
	cfg := targetConfig{
		bufferSize: defaultTargetBufferSize,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.bufferSize <= 0 {
		cfg.bufferSize = defaultTargetBufferSize
	}

	if strings.TrimSpace(targetPath) == "" {
		return nil, fmt.Errorf("target path cannot be empty")
	}

	if !isGCSPath(targetPath) {
		return newLocalTarget(targetPath)
	}

	commit := remoteCommitter
	return newStagedTarget(ctx, targetPath, cfg, commit)
}

var remoteCommitter = commitToGCS

type targetState uint8

const (
	targetStateOpen targetState = iota
	targetStateCommitted
	targetStateAborted
)

type stagedTarget struct {
	mu         sync.Mutex
	ctx        context.Context
	targetPath string
	tempPath   string
	tempFile   *os.File
	writer     *bufio.Writer
	bufferSize int
	commit     targetCommitFunc
	state      targetState
}

type localTarget struct {
	mu         sync.Mutex
	targetPath string
	file       *os.File
	state      targetState
}

func newLocalTarget(targetPath string) (*localTarget, error) {
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o700); err != nil {
		return nil, fmt.Errorf("create output directory: %w", err)
	}

	file, err := os.OpenFile(targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open output file: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		safety.Close(file, targetPath)
		return nil, fmt.Errorf("set output file permissions: %w", err)
	}

	return &localTarget{
		targetPath: targetPath,
		file:       file,
		state:      targetStateOpen,
	}, nil
}

func newStagedTarget(ctx context.Context, targetPath string, cfg targetConfig, commit targetCommitFunc) (*stagedTarget, error) {
	tempDir := cfg.tempDir
	if tempDir == "" {
		if isGCSPath(targetPath) {
			tempDir = os.TempDir()
		} else {
			tempDir = filepath.Dir(targetPath)
		}
	}

	if err := os.MkdirAll(tempDir, 0o700); err != nil {
		return nil, fmt.Errorf("create staging directory: %w", err)
	}

	tempFile, err := os.CreateTemp(tempDir, "data-refinery-output-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("create staged output file: %w", err)
	}

	return &stagedTarget{
		ctx:        ctx,
		targetPath: targetPath,
		tempPath:   tempFile.Name(),
		tempFile:   tempFile,
		writer:     bufio.NewWriterSize(tempFile, cfg.bufferSize),
		bufferSize: cfg.bufferSize,
		commit:     commit,
		state:      targetStateOpen,
	}, nil
}

func (t *stagedTarget) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != targetStateOpen {
		return 0, fmt.Errorf("target is already finalized")
	}
	if t.writer == nil {
		return 0, fmt.Errorf("target writer is not available")
	}

	written, err := t.writer.Write(p)
	if err != nil {
		return written, fmt.Errorf("write staged output: %w", err)
	}
	return written, nil
}

func (t *localTarget) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != targetStateOpen {
		return 0, fmt.Errorf("target is already finalized")
	}
	if t.file == nil {
		return 0, fmt.Errorf("target file is not available")
	}

	written, err := t.file.Write(p)
	if err != nil {
		return written, fmt.Errorf("write output file: %w", err)
	}
	return written, nil
}

func (t *stagedTarget) WriteLine(line []byte) error {
	if _, err := t.Write(line); err != nil {
		return err
	}
	_, err := t.Write([]byte{'\n'})
	return err
}

func (t *localTarget) WriteLine(line []byte) error {
	if _, err := t.Write(line); err != nil {
		return err
	}
	_, err := t.Write([]byte{'\n'})
	return err
}

func (t *stagedTarget) Commit(ctx context.Context) error {
	t.mu.Lock()
	if t.state == targetStateCommitted {
		t.mu.Unlock()
		return nil
	}
	if t.state == targetStateAborted {
		t.mu.Unlock()
		return nil
	}
	if err := t.flushAndCloseLocked(); err != nil {
		t.mu.Unlock()
		return err
	}
	tempPath := t.tempPath
	targetPath := t.targetPath
	commit := t.commit
	t.mu.Unlock()

	if commit == nil {
		return fmt.Errorf("target commit handler is not configured")
	}
	if err := commit(ctx, tempPath, targetPath); err != nil {
		return err
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state == targetStateAborted {
		return nil
	}
	if tempPath != "" {
		safety.Remove(tempPath, tempPath)
		t.tempPath = ""
	}
	t.state = targetStateCommitted
	return nil
}

func (t *localTarget) Commit(ctx context.Context) error {
	_ = ctx

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state == targetStateCommitted || t.state == targetStateAborted {
		return nil
	}
	if err := t.flushAndCloseLocked(); err != nil {
		return err
	}
	t.state = targetStateCommitted
	return nil
}

func (t *stagedTarget) Abort(ctx context.Context) error {
	_ = ctx

	t.mu.Lock()
	if t.state == targetStateCommitted {
		t.mu.Unlock()
		return nil
	}
	if t.state == targetStateAborted {
		t.mu.Unlock()
		return nil
	}
	err := t.flushAndCloseLocked()
	tempPath := t.tempPath
	t.tempPath = ""
	t.state = targetStateAborted
	t.mu.Unlock()

	if tempPath != "" {
		safety.Remove(tempPath, tempPath)
	}
	return err
}

func (t *localTarget) Abort(ctx context.Context) error {
	_ = ctx

	t.mu.Lock()
	if t.state == targetStateCommitted || t.state == targetStateAborted {
		t.mu.Unlock()
		return nil
	}
	err := t.flushAndCloseLocked()
	targetPath := t.targetPath
	t.state = targetStateAborted
	t.mu.Unlock()

	if targetPath != "" {
		safety.Remove(targetPath, targetPath)
	}
	return err
}

func (t *stagedTarget) flushAndCloseLocked() error {
	if t.writer != nil {
		if err := t.writer.Flush(); err != nil {
			return fmt.Errorf("flush staged output: %w", err)
		}
		t.writer = nil
	}
	if t.tempFile != nil {
		if err := t.tempFile.Close(); err != nil {
			return fmt.Errorf("close staged output file: %w", err)
		}
		t.tempFile = nil
	}
	return nil
}

func (t *localTarget) flushAndCloseLocked() error {
	if t.file != nil {
		if err := t.file.Close(); err != nil {
			return fmt.Errorf("close output file: %w", err)
		}
		t.file = nil
	}
	return nil
}

func isGCSPath(path string) bool {
	return strings.HasPrefix(strings.TrimSpace(path), "gs://")
}

func commitToGCS(ctx context.Context, tempPath, targetPath string) error {
	file, err := os.Open(tempPath)
	if err != nil {
		return fmt.Errorf("open staged output file: %w", err)
	}
	defer safety.Close(file, tempPath)

	writer, err := NewGCSWriter(ctx, targetPath)
	if err != nil {
		return fmt.Errorf("create GCS writer: %w", err)
	}
	defer safety.Close(writer, targetPath)

	if err := writer.Open(); err != nil {
		return fmt.Errorf("open GCS writer: %w", err)
	}
	if _, err := writer.StreamCopy(file); err != nil {
		return fmt.Errorf("stream staged output to GCS: %w", err)
	}

	return nil
}
