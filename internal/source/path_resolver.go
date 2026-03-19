// internal/source/path_resolver.go
package source

import (
	"fmt"
	"path/filepath"
	"strings"
)

// PathResolver handles path resolution and manipulation for different storage backends
type PathResolver struct {
	preserveStructure bool
	basePath          string
}

// NewPathResolver creates a new path resolver
func NewPathResolver(preserveStructure bool, basePath string) *PathResolver {
	return &PathResolver{
		preserveStructure: preserveStructure,
		basePath:          basePath,
	}
}

// GCSPathInfo contains information about a GCS path
type GCSPathInfo struct {
	FullPath   string
	Bucket     string
	ObjectName string
	Directory  string
	FileName   string
	Extension  string
}

// ParseGCSPath parses a GCS path into its components
func (pr *PathResolver) ParseGCSPath(gcsPath string) (*GCSPathInfo, error) {
	if !strings.HasPrefix(gcsPath, "gs://") {
		return nil, fmt.Errorf("invalid GCS path: must start with gs://")
	}

	trimmedPath := strings.TrimPrefix(gcsPath, "gs://")
	parts := strings.SplitN(trimmedPath, "/", 2)

	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid GCS path: must include bucket and object name")
	}

	bucket := parts[0]
	objectName := parts[1]

	if bucket == "" || objectName == "" {
		return nil, fmt.Errorf("bucket and object name cannot be empty")
	}

	// Parse directory and filename
	directory := filepath.Dir(objectName)
	if directory == "." {
		directory = ""
	}

	fileName := filepath.Base(objectName)
	extension := filepath.Ext(fileName)

	return &GCSPathInfo{
		FullPath:   gcsPath,
		Bucket:     bucket,
		ObjectName: objectName,
		Directory:  directory,
		FileName:   fileName,
		Extension:  extension,
	}, nil
}

// GenerateProcessedPath generates a processed file path maintaining the original structure
func (pr *PathResolver) GenerateProcessedPath(originalPath, suffix string) (string, error) {
	if strings.HasPrefix(originalPath, "gs://") {
		return pr.generateGCSProcessedPath(originalPath, suffix)
	}

	return pr.generateLocalProcessedPath(originalPath, suffix)
}

// generateGCSProcessedPath generates a processed GCS path
func (pr *PathResolver) generateGCSProcessedPath(originalPath, suffix string) (string, error) {
	pathInfo, err := pr.ParseGCSPath(originalPath)
	if err != nil {
		return "", fmt.Errorf("failed to parse GCS path: %w", err)
	}

	// Create new filename with suffix
	baseFileName := strings.TrimSuffix(pathInfo.FileName, pathInfo.Extension)
	newFileName := fmt.Sprintf("%s%s%s", baseFileName, suffix, pathInfo.Extension)

	// Construct new object name
	var newObjectName string
	if pathInfo.Directory != "" {
		newObjectName = fmt.Sprintf("%s/%s", pathInfo.Directory, newFileName)
	} else {
		newObjectName = newFileName
	}

	// Generate new GCS path
	newPath := fmt.Sprintf("gs://%s/%s", pathInfo.Bucket, newObjectName)

	return newPath, nil
}

// generateLocalProcessedPath generates a processed local path
func (pr *PathResolver) generateLocalProcessedPath(originalPath, suffix string) (string, error) {
	directory := filepath.Dir(originalPath)
	fileName := filepath.Base(originalPath)
	extension := filepath.Ext(fileName)
	baseFileName := strings.TrimSuffix(fileName, extension)

	newFileName := fmt.Sprintf("%s%s%s", baseFileName, suffix, extension)
	newPath := filepath.Join(directory, newFileName)

	return newPath, nil
}

// CreateBackupPath creates a backup path for a file
func (pr *PathResolver) CreateBackupPath(originalPath, backupDir string) (string, error) {
	if strings.HasPrefix(originalPath, "gs://") {
		return pr.createGCSBackupPath(originalPath, backupDir)
	}

	return pr.createLocalBackupPath(originalPath, backupDir)
}

// createGCSBackupPath creates a backup path for GCS files (converts to local)
func (pr *PathResolver) createGCSBackupPath(originalPath, backupDir string) (string, error) {
	pathInfo, err := pr.ParseGCSPath(originalPath)
	if err != nil {
		return "", fmt.Errorf("failed to parse GCS path: %w", err)
	}

	// Create local backup structure preserving GCS structure
	backupPath := filepath.Join(backupDir, "gcs", pathInfo.Bucket, pathInfo.ObjectName)

	return backupPath, nil
}

// createLocalBackupPath creates a backup path for local files
func (pr *PathResolver) createLocalBackupPath(originalPath, backupDir string) (string, error) {
	// Get absolute path
	absPath, err := filepath.Abs(originalPath)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path: %w", err)
	}

	// Create backup structure preserving directory structure
	relPath, err := filepath.Rel(pr.basePath, absPath)
	if err != nil {
		// If we can't get relative path, use the full path structure
		relPath = strings.TrimPrefix(absPath, "/")
	}

	backupPath := filepath.Join(backupDir, "local", relPath)

	return backupPath, nil
}

// PathMapping represents a mapping between original and processed paths
type PathMapping struct {
	OriginalPath  string
	ProcessedPath string
	BackupPath    string
	IsGCS         bool
	PathInfo      *GCSPathInfo
}

// CreatePathMapping creates a complete path mapping for a file
func (pr *PathResolver) CreatePathMapping(originalPath, suffix, backupDir string) (*PathMapping, error) {
	processedPath, err := pr.GenerateProcessedPath(originalPath, suffix)
	if err != nil {
		return nil, fmt.Errorf("failed to generate processed path: %w", err)
	}

	backupPath, err := pr.CreateBackupPath(originalPath, backupDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create backup path: %w", err)
	}

	mapping := &PathMapping{
		OriginalPath:  originalPath,
		ProcessedPath: processedPath,
		BackupPath:    backupPath,
		IsGCS:         strings.HasPrefix(originalPath, "gs://"),
	}

	// Parse GCS path info if applicable
	if mapping.IsGCS {
		pathInfo, err := pr.ParseGCSPath(originalPath)
		if err != nil {
			return nil, fmt.Errorf("failed to parse GCS path info: %w", err)
		}
		mapping.PathInfo = pathInfo
	}

	return mapping, nil
}

// ValidatePathMapping validates that a path mapping maintains structure consistency
func (pr *PathResolver) ValidatePathMapping(mapping *PathMapping) error {
	if mapping.IsGCS {
		return pr.validateGCSMapping(mapping)
	}

	return pr.validateLocalMapping(mapping)
}

// validateGCSMapping validates GCS path mapping
func (pr *PathResolver) validateGCSMapping(mapping *PathMapping) error {
	originalInfo, err := pr.ParseGCSPath(mapping.OriginalPath)
	if err != nil {
		return fmt.Errorf("failed to parse original path: %w", err)
	}

	processedInfo, err := pr.ParseGCSPath(mapping.ProcessedPath)
	if err != nil {
		return fmt.Errorf("failed to parse processed path: %w", err)
	}

	// Validate bucket consistency
	if originalInfo.Bucket != processedInfo.Bucket {
		return fmt.Errorf("bucket mismatch: original=%s, processed=%s",
			originalInfo.Bucket, processedInfo.Bucket)
	}

	// Validate directory consistency
	if originalInfo.Directory != processedInfo.Directory {
		return fmt.Errorf("directory mismatch: original=%s, processed=%s",
			originalInfo.Directory, processedInfo.Directory)
	}

	// Validate extension consistency
	if originalInfo.Extension != processedInfo.Extension {
		return fmt.Errorf("extension mismatch: original=%s, processed=%s",
			originalInfo.Extension, processedInfo.Extension)
	}

	return nil
}

// validateLocalMapping validates local path mapping
func (pr *PathResolver) validateLocalMapping(mapping *PathMapping) error {
	originalDir := filepath.Dir(mapping.OriginalPath)
	processedDir := filepath.Dir(mapping.ProcessedPath)

	// Validate directory consistency
	if originalDir != processedDir {
		return fmt.Errorf("directory mismatch: original=%s, processed=%s",
			originalDir, processedDir)
	}

	// Validate extension consistency
	originalExt := filepath.Ext(mapping.OriginalPath)
	processedExt := filepath.Ext(mapping.ProcessedPath)

	if originalExt != processedExt {
		return fmt.Errorf("extension mismatch: original=%s, processed=%s",
			originalExt, processedExt)
	}

	return nil
}

// GetRelativePath returns the relative path component of a full path
func (pr *PathResolver) GetRelativePath(fullPath, basePath string) (string, error) {
	if strings.HasPrefix(fullPath, "gs://") {
		pathInfo, err := pr.ParseGCSPath(fullPath)
		if err != nil {
			return "", err
		}
		return pathInfo.ObjectName, nil
	}

	return filepath.Rel(basePath, fullPath)
}

// NormalizePath normalizes a path for consistent comparison
func (pr *PathResolver) NormalizePath(path string) string {
	if strings.HasPrefix(path, "gs://") {
		return strings.ToLower(path)
	}

	return filepath.Clean(path)
}

// PathExists checks if a path exists (local only, for GCS use GCS client)
func (pr *PathResolver) PathExists(path string) bool {
	if strings.HasPrefix(path, "gs://") {
		// For GCS paths, this would require a GCS client check
		return false
	}

	_, err := filepath.Abs(path)
	return err == nil
}

// ExtractBaseName extracts the base name from a path without extension
func (pr *PathResolver) ExtractBaseName(path string) string {
	if strings.HasPrefix(path, "gs://") {
		pathInfo, err := pr.ParseGCSPath(path)
		if err != nil {
			return ""
		}
		return strings.TrimSuffix(pathInfo.FileName, pathInfo.Extension)
	}

	fileName := filepath.Base(path)
	return strings.TrimSuffix(fileName, filepath.Ext(fileName))
}

// CombinePaths combines path components appropriately for the storage type
func (pr *PathResolver) CombinePaths(base, additional string) string {
	if strings.HasPrefix(base, "gs://") {
		// For GCS paths, use forward slashes
		return strings.TrimSuffix(base, "/") + "/" + strings.TrimPrefix(additional, "/")
	}

	return filepath.Join(base, additional)
}
