// Package safety provides shared guards for wrapping errors and safe conversions.
package safety

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	"os"
	"time"
)

// Flusher is implemented by buffered writers that expose a flush operation.
type Flusher interface {
	Flush() error
}

// ManagedContext derives a cancellable child context from a parent context.
func ManagedContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(parent)
}

// Wrap annotates an error with the action being performed.
func Wrap(err error, action string) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%s: %w", action, err)
}

// ContextErr wraps the current context error with action context.
func ContextErr(ctx context.Context, action string) error {
	return Wrap(ctx.Err(), action)
}

// Close closes a resource and logs any cleanup failure.
func Close(closer io.Closer, label string) {
	if closer == nil {
		return
	}
	if err := closer.Close(); err != nil {
		log.Printf("cleanup error closing %s: %v", label, err)
	}
}

// Flush flushes a buffered resource and logs any cleanup failure.
func Flush(flusher Flusher, label string) {
	if flusher == nil {
		return
	}
	if err := flusher.Flush(); err != nil {
		log.Printf("cleanup error flushing %s: %v", label, err)
	}
}

// Remove deletes a path and ignores missing-file errors.
func Remove(path, label string) {
	if path == "" {
		return
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		log.Printf("cleanup error removing %s: %v", label, err)
	}
}

// SaturatingInt32 converts an int to int32 without overflowing.
func SaturatingInt32(value int) int32 {
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	if value < math.MinInt32 {
		return math.MinInt32
	}
	return int32(value)
}

// SaturatingInt32FromInt64 converts an int64 to int32 without overflowing.
func SaturatingInt32FromInt64(value int64) int32 {
	if value > math.MaxInt32 {
		return math.MaxInt32
	}
	if value < math.MinInt32 {
		return math.MinInt32
	}
	return int32(value)
}

// SaturatingInt64 converts a uint64 to int64 without overflowing.
func SaturatingInt64(value uint64) int64 {
	if value > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(value)
}

// SaturatingDuration converts a uint64 nanosecond count into a safe duration.
func SaturatingDuration(value uint64) time.Duration {
	return time.Duration(SaturatingInt64(value))
}

// RandomFloat64 returns a pseudo-random float64 in the range [0, 1).
func RandomFloat64() float64 {
	var raw [8]byte
	if _, err := crand.Read(raw[:]); err != nil {
		const maxValue = uint64(1 << 53)
		return float64(uint64(time.Now().UnixNano())%maxValue) / float64(maxValue)
	}

	return float64(binary.BigEndian.Uint64(raw[:])>>11) / float64(uint64(1)<<53)
}
