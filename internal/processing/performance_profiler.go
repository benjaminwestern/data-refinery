package processing

import (
	"context"
	"fmt"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"time"
)

type ProfileType string

const (
	ProfileTypeCPU       ProfileType = "cpu"
	ProfileTypeMemory    ProfileType = "memory"
	ProfileTypeBlock     ProfileType = "block"
	ProfileTypeMutex     ProfileType = "mutex"
	ProfileTypeGoroutine ProfileType = "goroutine"
	ProfileTypeTrace     ProfileType = "trace"
)

type PerformanceProfiler struct {
	config     ProfilerConfig
	profiles   map[ProfileType]*ProfileData
	mutex      sync.RWMutex
	collectors map[ProfileType]ProfileCollector
	detectors  []BottleneckDetector
	ctx        context.Context
	cancel     context.CancelFunc
}

type ProfilerConfig struct {
	EnableCPUProfiling    bool          `json:"enable_cpu_profiling"`
	EnableMemoryProfiling bool          `json:"enable_memory_profiling"`
	EnableBlockProfiling  bool          `json:"enable_block_profiling"`
	EnableMutexProfiling  bool          `json:"enable_mutex_profiling"`
	ProfilingInterval     time.Duration `json:"profiling_interval"`
	RetentionPeriod       time.Duration `json:"retention_period"`
	MaxSamples            int           `json:"max_samples"`
	ThresholdCPU          float64       `json:"threshold_cpu"`
	ThresholdMemory       int64         `json:"threshold_memory"`
	ThresholdLatency      time.Duration `json:"threshold_latency"`
}

type ProfileData struct {
	Type        ProfileType            `json:"type"`
	Timestamp   time.Time              `json:"timestamp"`
	Duration    time.Duration          `json:"duration"`
	Samples     []ProfileSample        `json:"samples"`
	Summary     ProfileSummary         `json:"summary"`
	Bottlenecks []DetectedBottleneck   `json:"bottlenecks"`
	Metadata    map[string]interface{} `json:"metadata"`
}

type ProfileSample struct {
	Timestamp  time.Time              `json:"timestamp"`
	Value      float64                `json:"value"`
	Context    map[string]interface{} `json:"context"`
	StackTrace []StackFrame           `json:"stack_trace,omitempty"`
}

type StackFrame struct {
	Function string `json:"function"`
	File     string `json:"file"`
	Line     int    `json:"line"`
}

type ProfileSummary struct {
	TotalSamples     int                     `json:"total_samples"`
	SamplingRate     float64                 `json:"sampling_rate"`
	TopFunctions     []FunctionProfile       `json:"top_functions"`
	ResourceUsage    ResourceUsageProfile    `json:"resource_usage"`
	PerformanceStats PerformanceStatsProfile `json:"performance_stats"`
}

type FunctionProfile struct {
	Name           string        `json:"name"`
	File           string        `json:"file"`
	Line           int           `json:"line"`
	SampleCount    int           `json:"sample_count"`
	TotalTime      time.Duration `json:"total_time"`
	SelfTime       time.Duration `json:"self_time"`
	PercentOfTotal float64       `json:"percent_of_total"`
}

type ResourceUsageProfile struct {
	CPUUsage       float64       `json:"cpu_usage"`
	MemoryUsage    int64         `json:"memory_usage"`
	GoroutineCount int           `json:"goroutine_count"`
	HeapSize       int64         `json:"heap_size"`
	HeapInUse      int64         `json:"heap_in_use"`
	GCCount        int64         `json:"gc_count"`
	GCPause        time.Duration `json:"gc_pause"`
}

type PerformanceStatsProfile struct {
	AverageLatency time.Duration `json:"average_latency"`
	MaxLatency     time.Duration `json:"max_latency"`
	MinLatency     time.Duration `json:"min_latency"`
	P95Latency     time.Duration `json:"p95_latency"`
	P99Latency     time.Duration `json:"p99_latency"`
	Throughput     float64       `json:"throughput"`
	ErrorRate      float64       `json:"error_rate"`
}

type DetectedBottleneck struct {
	Type        string                 `json:"type"`
	Severity    string                 `json:"severity"`
	Location    string                 `json:"location"`
	Description string                 `json:"description"`
	Impact      string                 `json:"impact"`
	Suggestion  string                 `json:"suggestion"`
	Timestamp   time.Time              `json:"timestamp"`
	Metadata    map[string]interface{} `json:"metadata"`
}

type ProfileCollector interface {
	CollectProfile(ctx context.Context, duration time.Duration) (*ProfileData, error)
	Type() ProfileType
}

type BottleneckDetector interface {
	DetectBottlenecks(profile *ProfileData) []DetectedBottleneck
	Name() string
}

type CPUProfileCollector struct {
	config ProfilerConfig
}

type MemoryProfileCollector struct {
	config ProfilerConfig
}

type BlockProfileCollector struct {
	config ProfilerConfig
}

type MutexProfileCollector struct {
	config ProfilerConfig
}

type GoroutineProfileCollector struct {
	config ProfilerConfig
}

type CPUBottleneckDetector struct {
	thresholds map[string]float64
}

type MemoryBottleneckDetector struct {
	thresholds map[string]int64
}

type GoroutineBottleneckDetector struct {
	thresholds map[string]int
}

type ContentionBottleneckDetector struct {
	thresholds map[string]time.Duration
}

func NewPerformanceProfiler(config ProfilerConfig) *PerformanceProfiler {
	ctx, cancel := context.WithCancel(context.Background())

	profiler := &PerformanceProfiler{
		config:     config,
		profiles:   make(map[ProfileType]*ProfileData),
		collectors: make(map[ProfileType]ProfileCollector),
		detectors:  make([]BottleneckDetector, 0),
		ctx:        ctx,
		cancel:     cancel,
	}

	profiler.setupCollectors()
	profiler.setupDetectors()

	return profiler
}

func (pp *PerformanceProfiler) setupCollectors() {
	if pp.config.EnableCPUProfiling {
		pp.collectors[ProfileTypeCPU] = &CPUProfileCollector{config: pp.config}
	}
	if pp.config.EnableMemoryProfiling {
		pp.collectors[ProfileTypeMemory] = &MemoryProfileCollector{config: pp.config}
	}
	if pp.config.EnableBlockProfiling {
		pp.collectors[ProfileTypeBlock] = &BlockProfileCollector{config: pp.config}
	}
	if pp.config.EnableMutexProfiling {
		pp.collectors[ProfileTypeMutex] = &MutexProfileCollector{config: pp.config}
	}
	pp.collectors[ProfileTypeGoroutine] = &GoroutineProfileCollector{config: pp.config}
}

func (pp *PerformanceProfiler) setupDetectors() {
	pp.detectors = append(pp.detectors,
		&CPUBottleneckDetector{
			thresholds: map[string]float64{
				"high_cpu":       pp.config.ThresholdCPU,
				"cpu_spike":      pp.config.ThresholdCPU * 1.5,
				"sustained_high": pp.config.ThresholdCPU * 0.8,
			},
		},
		&MemoryBottleneckDetector{
			thresholds: map[string]int64{
				"high_memory": pp.config.ThresholdMemory,
				"memory_leak": pp.config.ThresholdMemory * 2,
				"gc_pressure": pp.config.ThresholdMemory / 2,
			},
		},
		&GoroutineBottleneckDetector{
			thresholds: map[string]int{
				"goroutine_leak":  10000,
				"high_goroutines": 5000,
				"goroutine_spike": 2000,
			},
		},
		&ContentionBottleneckDetector{
			thresholds: map[string]time.Duration{
				"high_contention":  pp.config.ThresholdLatency,
				"blocking":         pp.config.ThresholdLatency * 2,
				"mutex_contention": pp.config.ThresholdLatency / 2,
			},
		},
	)
}

func (pp *PerformanceProfiler) Start() error {
	if pp.config.EnableBlockProfiling {
		runtime.SetBlockProfileRate(1)
	}
	if pp.config.EnableMutexProfiling {
		runtime.SetMutexProfileFraction(1)
	}

	go pp.collectProfiles()
	go pp.detectBottlenecks()
	go pp.cleanupOldProfiles()

	return nil
}

func (pp *PerformanceProfiler) Stop() error {
	pp.cancel()
	return nil
}

func (pp *PerformanceProfiler) collectProfiles() {
	ticker := time.NewTicker(pp.config.ProfilingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pp.ctx.Done():
			return
		case <-ticker.C:
			for profileType, collector := range pp.collectors {
				go func(pt ProfileType, c ProfileCollector) {
					profile, err := c.CollectProfile(pp.ctx, pp.config.ProfilingInterval)
					if err != nil {
						fmt.Printf("Error collecting %s profile: %v\n", pt, err)
						return
					}

					pp.storeProfile(profile)
				}(profileType, collector)
			}
		}
	}
}

func (pp *PerformanceProfiler) storeProfile(profile *ProfileData) {
	pp.mutex.Lock()
	defer pp.mutex.Unlock()

	pp.profiles[profile.Type] = profile
}

func (pp *PerformanceProfiler) detectBottlenecks() {
	ticker := time.NewTicker(pp.config.ProfilingInterval * 2)
	defer ticker.Stop()

	for {
		select {
		case <-pp.ctx.Done():
			return
		case <-ticker.C:
			pp.mutex.RLock()
			profiles := make(map[ProfileType]*ProfileData)
			for k, v := range pp.profiles {
				profiles[k] = v
			}
			pp.mutex.RUnlock()

			for _, profile := range profiles {
				for _, detector := range pp.detectors {
					bottlenecks := detector.DetectBottlenecks(profile)
					if len(bottlenecks) > 0 {
						pp.handleBottlenecks(profile, bottlenecks)
					}
				}
			}
		}
	}
}

func (pp *PerformanceProfiler) handleBottlenecks(profile *ProfileData, bottlenecks []DetectedBottleneck) {
	pp.mutex.Lock()
	defer pp.mutex.Unlock()

	if pp.profiles[profile.Type] != nil {
		pp.profiles[profile.Type].Bottlenecks = append(pp.profiles[profile.Type].Bottlenecks, bottlenecks...)
	}

	for _, bottleneck := range bottlenecks {
		fmt.Printf("Bottleneck detected: %s - %s\n", bottleneck.Type, bottleneck.Description)
	}
}

func (pp *PerformanceProfiler) cleanupOldProfiles() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-pp.ctx.Done():
			return
		case <-ticker.C:
			pp.mutex.Lock()
			cutoff := time.Now().Add(-pp.config.RetentionPeriod)

			for profileType, profile := range pp.profiles {
				if profile.Timestamp.Before(cutoff) {
					delete(pp.profiles, profileType)
				}
			}
			pp.mutex.Unlock()
		}
	}
}

func (pp *PerformanceProfiler) GetProfile(profileType ProfileType) (*ProfileData, bool) {
	pp.mutex.RLock()
	defer pp.mutex.RUnlock()

	profile, exists := pp.profiles[profileType]
	return profile, exists
}

func (pp *PerformanceProfiler) GetAllProfiles() map[ProfileType]*ProfileData {
	pp.mutex.RLock()
	defer pp.mutex.RUnlock()

	result := make(map[ProfileType]*ProfileData)
	for k, v := range pp.profiles {
		result[k] = v
	}
	return result
}

func (pp *PerformanceProfiler) GetBottlenecks() []DetectedBottleneck {
	pp.mutex.RLock()
	defer pp.mutex.RUnlock()

	var allBottlenecks []DetectedBottleneck
	for _, profile := range pp.profiles {
		allBottlenecks = append(allBottlenecks, profile.Bottlenecks...)
	}

	sort.Slice(allBottlenecks, func(i, j int) bool {
		return allBottlenecks[i].Timestamp.After(allBottlenecks[j].Timestamp)
	})

	return allBottlenecks
}

func (cpc *CPUProfileCollector) CollectProfile(ctx context.Context, duration time.Duration) (*ProfileData, error) {
	profile := &ProfileData{
		Type:      ProfileTypeCPU,
		Timestamp: time.Now(),
		Duration:  duration,
		Samples:   make([]ProfileSample, 0),
		Metadata:  make(map[string]interface{}),
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	profile.Summary = ProfileSummary{
		ResourceUsage: ResourceUsageProfile{
			CPUUsage:       getCPUUsage(),
			MemoryUsage:    int64(memStats.Alloc),
			GoroutineCount: runtime.NumGoroutine(),
			HeapSize:       int64(memStats.HeapSys),
			HeapInUse:      int64(memStats.HeapInuse),
			GCCount:        int64(memStats.NumGC),
			GCPause:        time.Duration(memStats.PauseNs[(memStats.NumGC+255)%256]),
		},
	}

	return profile, nil
}

func (cpc *CPUProfileCollector) Type() ProfileType {
	return ProfileTypeCPU
}

func (mpc *MemoryProfileCollector) CollectProfile(ctx context.Context, duration time.Duration) (*ProfileData, error) {
	profile := &ProfileData{
		Type:      ProfileTypeMemory,
		Timestamp: time.Now(),
		Duration:  duration,
		Samples:   make([]ProfileSample, 0),
		Metadata:  make(map[string]interface{}),
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	profile.Summary = ProfileSummary{
		ResourceUsage: ResourceUsageProfile{
			MemoryUsage: int64(memStats.Alloc),
			HeapSize:    int64(memStats.HeapSys),
			HeapInUse:   int64(memStats.HeapInuse),
			GCCount:     int64(memStats.NumGC),
			GCPause:     time.Duration(memStats.PauseNs[(memStats.NumGC+255)%256]),
		},
	}

	memProfile := pprof.Lookup("heap")
	if memProfile != nil {
		profile.Metadata["heap_profile"] = memProfile
	}

	return profile, nil
}

func (mpc *MemoryProfileCollector) Type() ProfileType {
	return ProfileTypeMemory
}

func (bpc *BlockProfileCollector) CollectProfile(ctx context.Context, duration time.Duration) (*ProfileData, error) {
	profile := &ProfileData{
		Type:      ProfileTypeBlock,
		Timestamp: time.Now(),
		Duration:  duration,
		Samples:   make([]ProfileSample, 0),
		Metadata:  make(map[string]interface{}),
	}

	blockProfile := pprof.Lookup("block")
	if blockProfile != nil {
		profile.Metadata["block_profile"] = blockProfile
	}

	return profile, nil
}

func (bpc *BlockProfileCollector) Type() ProfileType {
	return ProfileTypeBlock
}

func (mpc *MutexProfileCollector) CollectProfile(ctx context.Context, duration time.Duration) (*ProfileData, error) {
	profile := &ProfileData{
		Type:      ProfileTypeMutex,
		Timestamp: time.Now(),
		Duration:  duration,
		Samples:   make([]ProfileSample, 0),
		Metadata:  make(map[string]interface{}),
	}

	mutexProfile := pprof.Lookup("mutex")
	if mutexProfile != nil {
		profile.Metadata["mutex_profile"] = mutexProfile
	}

	return profile, nil
}

func (mpc *MutexProfileCollector) Type() ProfileType {
	return ProfileTypeMutex
}

func (gpc *GoroutineProfileCollector) CollectProfile(ctx context.Context, duration time.Duration) (*ProfileData, error) {
	profile := &ProfileData{
		Type:      ProfileTypeGoroutine,
		Timestamp: time.Now(),
		Duration:  duration,
		Samples:   make([]ProfileSample, 0),
		Metadata:  make(map[string]interface{}),
	}

	goroutineCount := runtime.NumGoroutine()

	profile.Summary = ProfileSummary{
		ResourceUsage: ResourceUsageProfile{
			GoroutineCount: goroutineCount,
		},
	}

	goroutineProfile := pprof.Lookup("goroutine")
	if goroutineProfile != nil {
		profile.Metadata["goroutine_profile"] = goroutineProfile
	}

	return profile, nil
}

func (gpc *GoroutineProfileCollector) Type() ProfileType {
	return ProfileTypeGoroutine
}

func (cbd *CPUBottleneckDetector) DetectBottlenecks(profile *ProfileData) []DetectedBottleneck {
	var bottlenecks []DetectedBottleneck

	cpuUsage := profile.Summary.ResourceUsage.CPUUsage

	if cpuUsage > cbd.thresholds["cpu_spike"] {
		bottlenecks = append(bottlenecks, DetectedBottleneck{
			Type:        "cpu_spike",
			Severity:    "critical",
			Location:    "system",
			Description: fmt.Sprintf("CPU usage spike detected: %.2f%%", cpuUsage),
			Impact:      "severe performance degradation",
			Suggestion:  "investigate CPU-intensive operations and optimize algorithms",
			Timestamp:   profile.Timestamp,
			Metadata:    map[string]interface{}{"cpu_usage": cpuUsage},
		})
	} else if cpuUsage > cbd.thresholds["high_cpu"] {
		bottlenecks = append(bottlenecks, DetectedBottleneck{
			Type:        "high_cpu",
			Severity:    "warning",
			Location:    "system",
			Description: fmt.Sprintf("High CPU usage detected: %.2f%%", cpuUsage),
			Impact:      "reduced system responsiveness",
			Suggestion:  "consider optimizing CPU-intensive operations",
			Timestamp:   profile.Timestamp,
			Metadata:    map[string]interface{}{"cpu_usage": cpuUsage},
		})
	}

	return bottlenecks
}

func (cbd *CPUBottleneckDetector) Name() string {
	return "CPUBottleneckDetector"
}

func (mbd *MemoryBottleneckDetector) DetectBottlenecks(profile *ProfileData) []DetectedBottleneck {
	var bottlenecks []DetectedBottleneck

	memoryUsage := profile.Summary.ResourceUsage.MemoryUsage
	heapSize := profile.Summary.ResourceUsage.HeapSize
	gcCount := profile.Summary.ResourceUsage.GCCount

	if memoryUsage > mbd.thresholds["memory_leak"] {
		bottlenecks = append(bottlenecks, DetectedBottleneck{
			Type:        "memory_leak",
			Severity:    "critical",
			Location:    "heap",
			Description: fmt.Sprintf("Potential memory leak detected: %d bytes", memoryUsage),
			Impact:      "memory exhaustion risk",
			Suggestion:  "investigate memory allocation patterns and fix leaks",
			Timestamp:   profile.Timestamp,
			Metadata:    map[string]interface{}{"memory_usage": memoryUsage, "heap_size": heapSize},
		})
	} else if memoryUsage > mbd.thresholds["high_memory"] {
		bottlenecks = append(bottlenecks, DetectedBottleneck{
			Type:        "high_memory",
			Severity:    "warning",
			Location:    "heap",
			Description: fmt.Sprintf("High memory usage detected: %d bytes", memoryUsage),
			Impact:      "potential memory pressure",
			Suggestion:  "consider memory optimization strategies",
			Timestamp:   profile.Timestamp,
			Metadata:    map[string]interface{}{"memory_usage": memoryUsage},
		})
	}

	if gcCount > 100 {
		bottlenecks = append(bottlenecks, DetectedBottleneck{
			Type:        "gc_pressure",
			Severity:    "info",
			Location:    "gc",
			Description: fmt.Sprintf("High GC activity detected: %d collections", gcCount),
			Impact:      "potential performance impact from frequent GC",
			Suggestion:  "optimize memory allocation patterns to reduce GC pressure",
			Timestamp:   profile.Timestamp,
			Metadata:    map[string]interface{}{"gc_count": gcCount},
		})
	}

	return bottlenecks
}

func (mbd *MemoryBottleneckDetector) Name() string {
	return "MemoryBottleneckDetector"
}

func (gbd *GoroutineBottleneckDetector) DetectBottlenecks(profile *ProfileData) []DetectedBottleneck {
	var bottlenecks []DetectedBottleneck

	goroutineCount := profile.Summary.ResourceUsage.GoroutineCount

	if goroutineCount > gbd.thresholds["goroutine_leak"] {
		bottlenecks = append(bottlenecks, DetectedBottleneck{
			Type:        "goroutine_leak",
			Severity:    "critical",
			Location:    "goroutines",
			Description: fmt.Sprintf("Potential goroutine leak detected: %d goroutines", goroutineCount),
			Impact:      "memory and CPU resource exhaustion",
			Suggestion:  "investigate goroutine lifecycle management and fix leaks",
			Timestamp:   profile.Timestamp,
			Metadata:    map[string]interface{}{"goroutine_count": goroutineCount},
		})
	} else if goroutineCount > gbd.thresholds["high_goroutines"] {
		bottlenecks = append(bottlenecks, DetectedBottleneck{
			Type:        "high_goroutines",
			Severity:    "warning",
			Location:    "goroutines",
			Description: fmt.Sprintf("High goroutine count detected: %d goroutines", goroutineCount),
			Impact:      "potential resource contention",
			Suggestion:  "review goroutine usage patterns and consider pooling",
			Timestamp:   profile.Timestamp,
			Metadata:    map[string]interface{}{"goroutine_count": goroutineCount},
		})
	}

	return bottlenecks
}

func (gbd *GoroutineBottleneckDetector) Name() string {
	return "GoroutineBottleneckDetector"
}

func (cbd *ContentionBottleneckDetector) DetectBottlenecks(profile *ProfileData) []DetectedBottleneck {
	var bottlenecks []DetectedBottleneck

	if profile.Type == ProfileTypeBlock || profile.Type == ProfileTypeMutex {
		bottlenecks = append(bottlenecks, DetectedBottleneck{
			Type:        "contention_detected",
			Severity:    "warning",
			Location:    "synchronization",
			Description: "Contention detected in synchronization primitives",
			Impact:      "reduced concurrency and performance",
			Suggestion:  "review locking strategies and consider lock-free alternatives",
			Timestamp:   profile.Timestamp,
			Metadata:    map[string]interface{}{"profile_type": profile.Type},
		})
	}

	return bottlenecks
}

func (cbd *ContentionBottleneckDetector) Name() string {
	return "ContentionBottleneckDetector"
}

func getCPUUsage() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(runtime.NumCPU() * 100)
}
