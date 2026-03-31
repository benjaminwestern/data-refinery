package analyser

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/benjaminwestern/data-refinery/internal/config"
	"github.com/benjaminwestern/data-refinery/internal/deletion"
	"github.com/benjaminwestern/data-refinery/internal/output"
	"github.com/benjaminwestern/data-refinery/internal/report"
	"github.com/benjaminwestern/data-refinery/internal/schema"
	"github.com/benjaminwestern/data-refinery/internal/search"
	"github.com/benjaminwestern/data-refinery/internal/source"
)

const smokeBucketName = "smoke-bucket"

type smokeObject struct {
	content     []byte
	contentType string
}

type smokeEnvironment struct {
	localDir         string
	remotePath       string
	localFolderSize  int64
	remoteFolderSize int64
	logDir           string
}

type fakeGCSServer struct {
	buckets map[string]map[string]smokeObject
}

func TestRemoteSourcesIntegration(t *testing.T) {
	env := newSmokeEnvironment(t)
	rep, baseName := env.runAnalysis(t, env.remotePath)

	assertSummaryBasics(t, rep, 2, 2, 5, env.remoteFolderSize, 1, 2)
	assertFolderDetail(t, rep, env.remotePath, 2, 2, 5, 5, env.remoteFolderSize)
	assertDuplicateGroupSize(t, rep, "2", 2)
	assertDuplicateRowHashCount(t, rep, 1, 2)
	assertAdvancedResults(t, rep, 9, 5, 5, 5, 3)
	assertStandardOutputs(t, baseName)
	assertAdvancedOutputs(t, env.logDir)
}

func newSmokeEnvironment(t *testing.T) *smokeEnvironment {
	t.Helper()

	rootDir := t.TempDir()
	localDir := filepath.Join(rootDir, "local")
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		t.Fatalf("failed to create local fixture dir: %v", err)
	}

	ordersA := strings.Join([]string{
		`{"id":"1","customer_id":"cust-123","product":{"name":"widget"},"line_items":[{"item_id":"li-001"},{"item_id":"li-099"}],"region":"north"}`,
		`{"id":"2","customer_id":"cust-999","product":{"name":"gadget"},"line_items":[{"item_id":"li-002"}],"region":"west"}`,
		`{"id":"3","customer_id":"cust-123","product":{"name":"widget"},"line_items":[{"item_id":"li-001"}],"region":"south"}`,
	}, "\n") + "\n"
	ordersB := strings.Join([]string{
		`{"id":"2","customer_id":"cust-999","product":{"name":"gadget"},"line_items":[{"item_id":"li-002"}],"region":"west"}`,
		`{"id":"4","customer_id":"cust-123","product":{"name":"widget"},"line_items":[{"item_id":"li-001"}],"region":"east"}`,
	}, "\n") + "\n"

	localAPath := filepath.Join(localDir, "orders_a.ndjson")
	localBPath := filepath.Join(localDir, "orders_b.ndjson")
	if err := os.WriteFile(localAPath, []byte(ordersA), 0o644); err != nil {
		t.Fatalf("failed to write local fixture A: %v", err)
	}
	if err := os.WriteFile(localBPath, []byte(ordersB), 0o644); err != nil {
		t.Fatalf("failed to write local fixture B: %v", err)
	}

	server := httptest.NewServer(&fakeGCSServer{
		buckets: map[string]map[string]smokeObject{
			smokeBucketName: {
				"remote/orders_a.ndjson": {content: []byte(ordersA), contentType: "application/x-ndjson"},
				"remote/orders_b.ndjson": {content: []byte(ordersB), contentType: "application/x-ndjson"},
			},
		},
	})
	t.Cleanup(server.Close)
	t.Setenv("STORAGE_EMULATOR_HOST", server.URL)

	return &smokeEnvironment{
		localDir:         localDir,
		remotePath:       fmt.Sprintf("gs://%s/remote", smokeBucketName),
		localFolderSize:  int64(len(ordersA) + len(ordersB)),
		remoteFolderSize: int64(len(ordersA) + len(ordersB)),
		logDir:           filepath.Join(rootDir, "logs"),
	}
}

func (e *smokeEnvironment) runAnalysis(t *testing.T, paths ...string) (*report.AnalysisReport, string) {
	t.Helper()

	ctx := context.Background()
	sources, err := source.DiscoverAll(ctx, paths)
	if err != nil {
		t.Fatalf("failed to discover sources: %v", err)
	}

	cfg := newSmokeConfig(e.logDir)
	if err := os.MkdirAll(cfg.LogPath, 0o755); err != nil {
		t.Fatalf("failed to create log dir: %v", err)
	}

	a, err := New(cfg)
	if err != nil {
		t.Fatalf("failed to create analyser: %v", err)
	}
	defer a.Close()

	rep := a.Run(ctx, sources)
	if rep == nil {
		t.Fatal("expected report to be non-nil")
	}

	baseName := report.SaveAndLog(rep, cfg.LogPath, cfg.EnableTxtOutput, cfg.EnableJSONOutput, cfg.CheckKey, cfg.CheckRow, cfg.ShowFolderBreakdown)
	if err := output.WriteAdvancedArtifacts(cfg.LogPath, cfg, rep); err != nil {
		t.Fatalf("failed to write advanced artifacts: %v", err)
	}

	return rep, baseName
}

func newSmokeConfig(logPath string) *config.Config {
	return &config.Config{
		Key:                 "id",
		Workers:             2,
		LogPath:             logPath,
		CheckKey:            true,
		CheckRow:            true,
		ShowFolderBreakdown: true,
		EnableTxtOutput:     true,
		EnableJSONOutput:    true,
		Advanced: &config.AdvancedConfig{
			SearchTargets: []config.SearchTarget{
				{
					Name:         "customer_search",
					Type:         "direct",
					Path:         "customer_id",
					TargetValues: []string{"cust-123"},
				},
				{
					Name:          "product_search",
					Type:          "nested_object",
					Path:          "product.name",
					TargetValues:  []string{"widget"},
					CaseSensitive: true,
				},
				{
					Name:         "line_item_search",
					Type:         "nested_array",
					Path:         "line_items[*].item_id",
					TargetValues: []string{"li-001"},
				},
			},
			HashingStrategy: config.HashingStrategy{
				Mode:        "selective",
				IncludeKeys: []string{"id", "customer_id", "product.name"},
			},
			SchemaDiscovery: config.SchemaDiscoveryConfig{
				Enabled:       true,
				SamplePercent: 1,
				MaxDepth:      5,
				MaxSamples:    100,
				OutputFormats: []string{"json", "csv"},
				GroupByFolder: true,
			},
			DeletionRules: []config.DeletionRule{
				{
					SearchTarget: "customer_search",
					Action:       "mark_for_deletion",
					OutputPath:   filepath.Join(logPath, "marked_customers.jsonl"),
				},
			},
		},
	}
}

func assertSummaryBasics(t *testing.T, rep *report.AnalysisReport, filesProcessed, totalFiles int32, rowsProcessed int64, bytesProcessed int64, uniqueDuplicatedKeys int, duplicateRowInstances int) {
	t.Helper()

	if rep.Summary.FilesProcessed != filesProcessed {
		t.Fatalf("expected %d processed files, got %d", filesProcessed, rep.Summary.FilesProcessed)
	}
	if rep.Summary.TotalFiles != int(totalFiles) {
		t.Fatalf("expected %d total files, got %d", totalFiles, rep.Summary.TotalFiles)
	}
	if rep.Summary.TotalRowsProcessed != rowsProcessed {
		t.Fatalf("expected %d processed rows, got %d", rowsProcessed, rep.Summary.TotalRowsProcessed)
	}
	if rep.Summary.TotalKeyOccurrences != int(rowsProcessed) {
		t.Fatalf("expected %d key occurrences, got %d", rowsProcessed, rep.Summary.TotalKeyOccurrences)
	}
	if rep.Summary.ProcessedDataSizeBytes != bytesProcessed {
		t.Fatalf("expected %d processed bytes, got %d", bytesProcessed, rep.Summary.ProcessedDataSizeBytes)
	}
	if rep.Summary.TotalDataSizeOverallBytes != bytesProcessed {
		t.Fatalf("expected %d total bytes, got %d", bytesProcessed, rep.Summary.TotalDataSizeOverallBytes)
	}
	if rep.Summary.UniqueKeysDuplicated != uniqueDuplicatedKeys {
		t.Fatalf("expected %d duplicated keys, got %d", uniqueDuplicatedKeys, rep.Summary.UniqueKeysDuplicated)
	}
	if rep.Summary.DuplicateRowInstances != duplicateRowInstances {
		t.Fatalf("expected %d duplicate row instances, got %d", duplicateRowInstances, rep.Summary.DuplicateRowInstances)
	}
}

func assertFolderDetail(t *testing.T, rep *report.AnalysisReport, folder string, filesProcessed, totalFiles, rowsProcessed, keysFound int, folderBytes int64) {
	t.Helper()

	detail, ok := rep.Summary.FolderDetails[folder]
	if !ok {
		t.Fatalf("expected folder detail for %s", folder)
	}
	if detail.FilesProcessed != filesProcessed {
		t.Fatalf("expected %d processed files in %s, got %d", filesProcessed, folder, detail.FilesProcessed)
	}
	if detail.TotalFiles != totalFiles {
		t.Fatalf("expected %d total files in %s, got %d", totalFiles, folder, detail.TotalFiles)
	}
	if detail.RowsProcessed != rowsProcessed {
		t.Fatalf("expected %d processed rows in %s, got %d", rowsProcessed, folder, detail.RowsProcessed)
	}
	if detail.KeysFound != keysFound {
		t.Fatalf("expected %d keys found in %s, got %d", keysFound, folder, detail.KeysFound)
	}
	if detail.TotalSizeBytes != folderBytes {
		t.Fatalf("expected %d total bytes in %s, got %d", folderBytes, folder, detail.TotalSizeBytes)
	}
	if detail.ProcessedSizeBytes != folderBytes {
		t.Fatalf("expected %d processed bytes in %s, got %d", folderBytes, folder, detail.ProcessedSizeBytes)
	}
}

func assertDuplicateGroupSize(t *testing.T, rep *report.AnalysisReport, id string, expected int) {
	t.Helper()

	locations, ok := rep.DuplicateIDs[id]
	if !ok {
		t.Fatalf("expected duplicate ID group %s", id)
	}
	if len(locations) != expected {
		t.Fatalf("expected %d duplicate locations for ID %s, got %d", expected, id, len(locations))
	}
}

func assertDuplicateRowHashCount(t *testing.T, rep *report.AnalysisReport, expectedGroups, expectedLocations int) {
	t.Helper()

	if len(rep.DuplicateRows) != expectedGroups {
		t.Fatalf("expected %d duplicate row groups, got %d", expectedGroups, len(rep.DuplicateRows))
	}

	totalLocations := 0
	for _, locations := range rep.DuplicateRows {
		totalLocations += len(locations)
	}
	if totalLocations != expectedLocations {
		t.Fatalf("expected %d duplicate row locations, got %d", expectedLocations, totalLocations)
	}
}

func assertAdvancedResults(t *testing.T, rep *report.AnalysisReport, totalMatches int, processedRows int64, schemaRows int64, sampledRows int64, modifiedRows int64) {
	t.Helper()

	searchResults := searchResultsFromReport(t, rep)
	if searchResults.Summary.TotalMatches != totalMatches {
		t.Fatalf("expected %d total search matches, got %d", totalMatches, searchResults.Summary.TotalMatches)
	}
	if searchResults.Summary.ProcessedRows != processedRows {
		t.Fatalf("expected %d processed search rows, got %d", processedRows, searchResults.Summary.ProcessedRows)
	}

	schemaReport := schemaReportFromReport(t, rep)
	if schemaReport.TotalRows != schemaRows {
		t.Fatalf("expected %d schema total rows, got %d", schemaRows, schemaReport.TotalRows)
	}
	if schemaReport.SampledRows != sampledRows {
		t.Fatalf("expected %d schema sampled rows, got %d", sampledRows, schemaReport.SampledRows)
	}

	deletionStats := deletionStatsFromReport(t, rep)
	if deletionStats.ModifiedRows != modifiedRows {
		t.Fatalf("expected %d modified rows, got %d", modifiedRows, deletionStats.ModifiedRows)
	}
	if deletionStats.EndTime.IsZero() {
		t.Fatal("expected deletion stats end time to be populated")
	}
}

func assertStandardOutputs(t *testing.T, baseName string) {
	t.Helper()

	paths := []string{
		baseName + "_summary.txt",
		baseName + "_details.txt",
		baseName + ".json",
	}
	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected report output %s to exist: %v", path, err)
		}
	}
}

func assertAdvancedOutputs(t *testing.T, logDir string) {
	t.Helper()

	patterns := []string{
		"search_results_*.json",
		"search_target_customer_search_*.json",
		"search_target_product_search_*.json",
		"search_target_line_item_search_*.json",
		"schema_report_*.json",
		"schema_report_*.csv",
		"deletion_stats_*.json",
		"deletion_summary_*.txt",
	}

	for _, pattern := range patterns {
		matches, err := filepath.Glob(filepath.Join(logDir, pattern))
		if err != nil {
			t.Fatalf("failed to glob %s: %v", pattern, err)
		}
		if len(matches) == 0 {
			t.Fatalf("expected at least one file matching %s in %s", pattern, logDir)
		}
	}
}

func searchResultsFromReport(t *testing.T, rep *report.AnalysisReport) *search.Results {
	t.Helper()

	switch typed := rep.SearchResults.(type) {
	case *search.Results:
		return typed
	case search.Results:
		return &typed
	default:
		t.Fatalf("unexpected search results type: %T", rep.SearchResults)
		return nil
	}
}

func schemaReportFromReport(t *testing.T, rep *report.AnalysisReport) *schema.Report {
	t.Helper()

	switch typed := rep.SchemaReport.(type) {
	case *schema.Report:
		return typed
	case schema.Report:
		return &typed
	default:
		t.Fatalf("unexpected schema report type: %T", rep.SchemaReport)
		return nil
	}
}

func deletionStatsFromReport(t *testing.T, rep *report.AnalysisReport) *deletion.Stats {
	t.Helper()

	switch typed := rep.DeletionStats.(type) {
	case *deletion.Stats:
		return typed
	case deletion.Stats:
		return &typed
	default:
		t.Fatalf("unexpected deletion stats type: %T", rep.DeletionStats)
		return nil
	}
}

func (f *fakeGCSServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/storage/v1/b/"):
		f.handleStorageAPI(w, r)
	case strings.HasPrefix(r.URL.Path, "/download/storage/v1/b/"):
		f.handleDownloadAPI(w, r)
	default:
		f.handleObjectRead(w, r)
	}
}

func (f *fakeGCSServer) handleStorageAPI(w http.ResponseWriter, r *http.Request) {
	remainder := strings.TrimPrefix(r.URL.Path, "/storage/v1/b/")
	if strings.HasSuffix(remainder, "/o") {
		bucket := strings.TrimSuffix(remainder, "/o")
		bucket, _ = url.PathUnescape(strings.Trim(bucket, "/"))
		f.writeObjectList(w, bucket, r.URL.Query().Get("prefix"))
		return
	}

	bucket := strings.Trim(remainder, "/")
	bucket, _ = url.PathUnescape(bucket)
	if _, ok := f.buckets[bucket]; !ok {
		http.NotFound(w, r)
		return
	}

	writeJSON(w, map[string]any{
		"kind":          "storage#bucket",
		"id":            bucket,
		"name":          bucket,
		"location":      "US-CENTRAL1",
		"storageClass":  "STANDARD",
		"projectNumber": "0",
	})
}

func (f *fakeGCSServer) handleDownloadAPI(w http.ResponseWriter, r *http.Request) {
	remainder := strings.TrimPrefix(r.URL.Path, "/download/storage/v1/b/")
	parts := strings.SplitN(remainder, "/o/", 2)
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}
	bucket, _ := url.PathUnescape(parts[0])
	objectName, _ := url.PathUnescape(parts[1])
	f.writeObject(w, r, bucket, objectName)
}

func (f *fakeGCSServer) handleObjectRead(w http.ResponseWriter, r *http.Request) {
	escapedPath := strings.TrimPrefix(r.URL.EscapedPath(), "/")
	parts := strings.SplitN(escapedPath, "/", 2)
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}
	bucket, _ := url.PathUnescape(parts[0])
	objectName, _ := url.PathUnescape(parts[1])
	f.writeObject(w, r, bucket, objectName)
}

func (f *fakeGCSServer) writeObjectList(w http.ResponseWriter, bucket, prefix string) {
	objects, ok := f.buckets[bucket]
	if !ok {
		http.NotFound(w, nil)
		return
	}

	names := make([]string, 0, len(objects))
	for name := range objects {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	items := make([]map[string]any, 0, len(names))
	for _, name := range names {
		obj := objects[name]
		items = append(items, map[string]any{
			"kind":        "storage#object",
			"name":        name,
			"id":          fmt.Sprintf("%s/%s", bucket, name),
			"bucket":      bucket,
			"size":        fmt.Sprintf("%d", len(obj.content)),
			"contentType": obj.contentType,
		})
	}

	writeJSON(w, map[string]any{
		"kind":  "storage#objects",
		"items": items,
	})
}

func (f *fakeGCSServer) writeObject(w http.ResponseWriter, r *http.Request, bucket, objectName string) {
	objects, ok := f.buckets[bucket]
	if !ok {
		http.NotFound(w, r)
		return
	}
	obj, ok := objects[objectName]
	if !ok {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", obj.contentType)
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(obj.content)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(obj.content)
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
