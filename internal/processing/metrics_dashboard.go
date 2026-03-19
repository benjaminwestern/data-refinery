package processing

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MetricType string

const (
	MetricTypeCounter   MetricType = "counter"
	MetricTypeGauge     MetricType = "gauge"
	MetricTypeHistogram MetricType = "histogram"
	MetricTypeSummary   MetricType = "summary"
)

const (
	defaultHTTPAPIBindAddress = "127.0.0.1"
	defaultHTTPReadTimeout    = 10 * time.Second
	defaultHTTPReadHeaderTime = 5 * time.Second
	defaultHTTPWriteTimeout   = 30 * time.Second
	defaultHTTPIdleTimeout    = 60 * time.Second
	defaultHTTPMaxHeaderBytes = 1 << 20
)

type DashboardMetric struct {
	Name      string                 `json:"name"`
	Type      MetricType             `json:"type"`
	Value     float64                `json:"value"`
	Labels    map[string]string      `json:"labels,omitempty"`
	Timestamp time.Time              `json:"timestamp"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
}

type MetricDataPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

type MetricSeries struct {
	Name       string            `json:"name"`
	Labels     map[string]string `json:"labels"`
	DataPoints []MetricDataPoint `json:"data_points"`
	Aggregated AggregatedMetrics `json:"aggregated"`
}

type AggregatedMetrics struct {
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Mean   float64 `json:"mean"`
	Median float64 `json:"median"`
	P95    float64 `json:"p95"`
	P99    float64 `json:"p99"`
	StdDev float64 `json:"std_dev"`
	Count  int     `json:"count"`
	Sum    float64 `json:"sum"`
	Rate   float64 `json:"rate"`
	Trend  string  `json:"trend"`
}

type DashboardConfig struct {
	RefreshInterval    time.Duration `json:"refresh_interval"`
	MaxDataPoints      int           `json:"max_data_points"`
	RetentionDuration  time.Duration `json:"retention_duration"`
	EnableHTTPAPI      bool          `json:"enable_http_api"`
	HTTPAPIPort        int           `json:"http_api_port"`
	HTTPAPIBindAddress string        `json:"http_api_bind_address,omitempty"`
	HTTPAPIAuthToken   string        `json:"http_api_auth_token,omitempty"`
	EnableHistoricalDB bool          `json:"enable_historical_db"`
	DBPath             string        `json:"db_path"`
}

type MetricsDashboard struct {
	config      DashboardConfig
	metrics     map[string]*MetricSeries
	mutex       sync.RWMutex
	collectors  []MetricCollector
	subscribers map[string]chan<- *DashboardMetric
	subMutex    sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
	httpServer  *http.Server
	queryEngine *QueryEngine
	historyDB   *HistoryDatabase
}

type MetricCollector interface {
	CollectMetrics(ctx context.Context) []*DashboardMetric
	Name() string
	Interval() time.Duration
}

type QueryEngine struct {
	dashboard *MetricsDashboard
}

type HistoryDatabase struct {
	path    string
	metrics map[string][]MetricDataPoint
	mutex   sync.RWMutex
}

func NewMetricsDashboard(config DashboardConfig) *MetricsDashboard {
	ctx, cancel := context.WithCancel(context.Background())

	dashboard := &MetricsDashboard{
		config:      config,
		metrics:     make(map[string]*MetricSeries),
		collectors:  make([]MetricCollector, 0),
		subscribers: make(map[string]chan<- *DashboardMetric),
		ctx:         ctx,
		cancel:      cancel,
		queryEngine: &QueryEngine{},
		historyDB: &HistoryDatabase{
			path:    config.DBPath,
			metrics: make(map[string][]MetricDataPoint),
		},
	}

	dashboard.queryEngine.dashboard = dashboard

	if config.EnableHTTPAPI {
		dashboard.setupHTTPServer()
	}

	return dashboard
}

func (md *MetricsDashboard) Start() error {
	go md.collectMetrics()
	go md.cleanupOldData()

	if md.config.EnableHTTPAPI {
		go func() {
			if err := md.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				fmt.Printf("HTTP server error: %v\n", err)
			}
		}()
	}

	return nil
}

func (md *MetricsDashboard) Stop() error {
	md.cancel()

	if md.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return md.httpServer.Shutdown(ctx)
	}

	return nil
}

func (md *MetricsDashboard) AddCollector(collector MetricCollector) {
	md.mutex.Lock()
	defer md.mutex.Unlock()
	md.collectors = append(md.collectors, collector)
}

func (md *MetricsDashboard) AddMetric(metric *DashboardMetric) {
	md.mutex.Lock()
	defer md.mutex.Unlock()

	key := md.getMetricKey(metric)
	series, exists := md.metrics[key]

	if !exists {
		series = &MetricSeries{
			Name:       metric.Name,
			Labels:     metric.Labels,
			DataPoints: make([]MetricDataPoint, 0),
		}
		md.metrics[key] = series
	}

	dataPoint := MetricDataPoint{
		Timestamp: metric.Timestamp,
		Value:     metric.Value,
	}

	series.DataPoints = append(series.DataPoints, dataPoint)

	if len(series.DataPoints) > md.config.MaxDataPoints {
		series.DataPoints = series.DataPoints[1:]
	}

	md.updateAggregatedMetrics(series)
	md.notifySubscribers(metric)

	if md.config.EnableHistoricalDB {
		md.historyDB.StoreMetric(key, dataPoint)
	}
}

func (md *MetricsDashboard) GetMetrics() map[string]*MetricSeries {
	md.mutex.RLock()
	defer md.mutex.RUnlock()

	result := make(map[string]*MetricSeries)
	for k, v := range md.metrics {
		result[k] = v
	}
	return result
}

func (md *MetricsDashboard) getMetricKey(metric *DashboardMetric) string {
	if len(metric.Labels) == 0 {
		return metric.Name
	}

	var labelPairs []string
	for k, v := range metric.Labels {
		labelPairs = append(labelPairs, fmt.Sprintf("%s=%s", k, v))
	}
	sort.Strings(labelPairs)

	return fmt.Sprintf("%s{%s}", metric.Name, strings.Join(labelPairs, ","))
}

func (md *MetricsDashboard) updateAggregatedMetrics(series *MetricSeries) {
	if len(series.DataPoints) == 0 {
		return
	}

	values := make([]float64, len(series.DataPoints))
	sum := 0.0

	for i, dp := range series.DataPoints {
		values[i] = dp.Value
		sum += dp.Value
	}

	sort.Float64s(values)

	n := len(values)
	series.Aggregated.Count = n
	series.Aggregated.Sum = sum
	series.Aggregated.Min = values[0]
	series.Aggregated.Max = values[n-1]
	series.Aggregated.Mean = sum / float64(n)

	if n%2 == 0 {
		series.Aggregated.Median = (values[n/2-1] + values[n/2]) / 2
	} else {
		series.Aggregated.Median = values[n/2]
	}

	if n > 1 {
		series.Aggregated.P95 = values[int(0.95*float64(n))]
		series.Aggregated.P99 = values[int(0.99*float64(n))]
	}

	variance := 0.0
	for _, v := range values {
		variance += math.Pow(v-series.Aggregated.Mean, 2)
	}
	series.Aggregated.StdDev = math.Sqrt(variance / float64(n))

	if len(series.DataPoints) >= 2 {
		duration := series.DataPoints[n-1].Timestamp.Sub(series.DataPoints[0].Timestamp)
		if duration > 0 {
			series.Aggregated.Rate = float64(n) / duration.Seconds()
		}
	}

	series.Aggregated.Trend = md.calculateTrend(series.DataPoints)
}

func (md *MetricsDashboard) calculateTrend(dataPoints []MetricDataPoint) string {
	if len(dataPoints) < 2 {
		return "stable"
	}

	n := len(dataPoints)
	recent := dataPoints[n-min(n, 5):]

	if len(recent) < 2 {
		return "stable"
	}

	sumX, sumY, sumXY, sumXX := 0.0, 0.0, 0.0, 0.0

	for i, dp := range recent {
		x := float64(i)
		y := dp.Value
		sumX += x
		sumY += y
		sumXY += x * y
		sumXX += x * x
	}

	nf := float64(len(recent))
	slope := (nf*sumXY - sumX*sumY) / (nf*sumXX - sumX*sumX)

	if slope > 0.1 {
		return "increasing"
	} else if slope < -0.1 {
		return "decreasing"
	}
	return "stable"
}

func (md *MetricsDashboard) Subscribe(name string, ch chan<- *DashboardMetric) {
	md.subMutex.Lock()
	defer md.subMutex.Unlock()
	md.subscribers[name] = ch
}

func (md *MetricsDashboard) Unsubscribe(name string) {
	md.subMutex.Lock()
	defer md.subMutex.Unlock()
	delete(md.subscribers, name)
}

func (md *MetricsDashboard) notifySubscribers(metric *DashboardMetric) {
	md.subMutex.RLock()
	defer md.subMutex.RUnlock()

	for _, ch := range md.subscribers {
		select {
		case ch <- metric:
		default:
		}
	}
}

func (md *MetricsDashboard) collectMetrics() {
	ticker := time.NewTicker(md.config.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-md.ctx.Done():
			return
		case <-ticker.C:
			for _, collector := range md.collectors {
				go func(c MetricCollector) {
					metrics := c.CollectMetrics(md.ctx)
					for _, metric := range metrics {
						md.AddMetric(metric)
					}
				}(collector)
			}
		}
	}
}

func (md *MetricsDashboard) cleanupOldData() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-md.ctx.Done():
			return
		case <-ticker.C:
			md.mutex.Lock()
			cutoff := time.Now().Add(-md.config.RetentionDuration)

			for key, series := range md.metrics {
				filtered := make([]MetricDataPoint, 0)
				for _, dp := range series.DataPoints {
					if dp.Timestamp.After(cutoff) {
						filtered = append(filtered, dp)
					}
				}
				series.DataPoints = filtered

				if len(series.DataPoints) == 0 {
					delete(md.metrics, key)
				} else {
					md.updateAggregatedMetrics(series)
				}
			}
			md.mutex.Unlock()
		}
	}
}

func (md *MetricsDashboard) setupHTTPServer() {
	mux := http.NewServeMux()

	mux.HandleFunc("/metrics", md.handleMetrics)
	mux.HandleFunc("/metrics/query", md.handleQuery)
	mux.HandleFunc("/metrics/series", md.handleSeries)
	mux.HandleFunc("/dashboard", md.handleDashboard)

	handler := http.Handler(mux)
	if token := strings.TrimSpace(md.config.HTTPAPIAuthToken); token != "" {
		handler = md.requireBearerAuth(handler, token)
	}

	md.httpServer = &http.Server{
		Addr:              net.JoinHostPort(resolveHTTPAPIBindAddress(md.config), strconv.Itoa(md.config.HTTPAPIPort)),
		Handler:           handler,
		ReadHeaderTimeout: defaultHTTPReadHeaderTime,
		ReadTimeout:       defaultHTTPReadTimeout,
		WriteTimeout:      defaultHTTPWriteTimeout,
		IdleTimeout:       defaultHTTPIdleTimeout,
		MaxHeaderBytes:    defaultHTTPMaxHeaderBytes,
	}
}

func (md *MetricsDashboard) requireBearerAuth(next http.Handler, token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+token {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func resolveHTTPAPIBindAddress(config DashboardConfig) string {
	bindAddress := strings.TrimSpace(config.HTTPAPIBindAddress)
	if bindAddress == "" {
		return defaultHTTPAPIBindAddress
	}

	if isLoopbackAddress(bindAddress) || strings.TrimSpace(config.HTTPAPIAuthToken) != "" {
		return bindAddress
	}

	return defaultHTTPAPIBindAddress
}

func isLoopbackAddress(address string) bool {
	if strings.EqualFold(address, "localhost") {
		return true
	}

	parsedIP := net.ParseIP(address)
	return parsedIP != nil && parsedIP.IsLoopback()
}

func (md *MetricsDashboard) handleMetrics(w http.ResponseWriter, r *http.Request) {
	md.mutex.RLock()
	defer md.mutex.RUnlock()

	w.Header().Set("Content-Type", "application/json")

	switch r.Method {
	case http.MethodGet:
		metrics := make([]*MetricSeries, 0, len(md.metrics))
		for _, series := range md.metrics {
			metrics = append(metrics, series)
		}
		json.NewEncoder(w).Encode(metrics)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (md *MetricsDashboard) handleQuery(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	query := r.URL.Query().Get("q")
	if query == "" {
		http.Error(w, "Query parameter 'q' is required", http.StatusBadRequest)
		return
	}

	result, err := md.queryEngine.Execute(query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusBadRequest)
		return
	}

	json.NewEncoder(w).Encode(result)
}

func (md *MetricsDashboard) handleSeries(w http.ResponseWriter, r *http.Request) {
	md.mutex.RLock()
	defer md.mutex.RUnlock()

	w.Header().Set("Content-Type", "application/json")

	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "Name parameter is required", http.StatusBadRequest)
		return
	}

	var matchingSeries []*MetricSeries
	for key, series := range md.metrics {
		if strings.Contains(key, name) {
			matchingSeries = append(matchingSeries, series)
		}
	}

	json.NewEncoder(w).Encode(matchingSeries)
}

func (md *MetricsDashboard) handleDashboard(w http.ResponseWriter, r *http.Request) {
	html := `<!DOCTYPE html>
<html>
<head>
    <title>Metrics Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .metric-card { border: 1px solid #ddd; padding: 20px; margin: 10px; border-radius: 5px; }
        .metric-value { font-size: 24px; font-weight: bold; }
        .metric-trend { font-size: 12px; color: #666; }
        .chart-container { width: 400px; height: 300px; }
        table { width: 100%; border-collapse: collapse; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
    </style>
</head>
<body>
    <h1>Real-time Metrics Dashboard</h1>
    <div id="metrics"></div>
    <script>
        async function fetchMetrics() {
            try {
                const response = await fetch('/metrics');
                const metrics = await response.json();
                displayMetrics(metrics);
            } catch (error) {
                console.error('Error fetching metrics:', error);
            }
        }
        
        function displayMetrics(metrics) {
            const container = document.getElementById('metrics');
            container.innerHTML = '';
            
            const table = document.createElement('table');
            table.innerHTML = '<tr><th>Metric</th><th>Value</th><th>Trend</th><th>Min</th><th>Max</th><th>Mean</th></tr>';
            
            metrics.forEach(metric => {
                const row = table.insertRow();
                row.innerHTML = '<td>' + metric.name + '</td><td>' + metric.aggregated.sum.toFixed(2) + '</td><td>' + metric.aggregated.trend + '</td><td>' + metric.aggregated.min.toFixed(2) + '</td><td>' + metric.aggregated.max.toFixed(2) + '</td><td>' + metric.aggregated.mean.toFixed(2) + '</td>';
            });
            
            container.appendChild(table);
        }
        
        fetchMetrics();
        setInterval(fetchMetrics, 5000);
    </script>
</body>
</html>`

	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(html))
}

func (qe *QueryEngine) Execute(query string) (interface{}, error) {
	parts := strings.Fields(query)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid query format")
	}

	function := parts[0]
	metricName := parts[1]

	qe.dashboard.mutex.RLock()
	defer qe.dashboard.mutex.RUnlock()

	var values []float64
	for key, series := range qe.dashboard.metrics {
		if strings.Contains(key, metricName) {
			for _, dp := range series.DataPoints {
				values = append(values, dp.Value)
			}
		}
	}

	if len(values) == 0 {
		return 0.0, nil
	}

	switch function {
	case "avg":
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values)), nil
	case "max":
		max := values[0]
		for _, v := range values {
			if v > max {
				max = v
			}
		}
		return max, nil
	case "min":
		min := values[0]
		for _, v := range values {
			if v < min {
				min = v
			}
		}
		return min, nil
	case "sum":
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum, nil
	case "count":
		return float64(len(values)), nil
	default:
		return nil, fmt.Errorf("unknown function: %s", function)
	}
}

func (hdb *HistoryDatabase) StoreMetric(key string, dataPoint MetricDataPoint) {
	hdb.mutex.Lock()
	defer hdb.mutex.Unlock()

	if _, exists := hdb.metrics[key]; !exists {
		hdb.metrics[key] = make([]MetricDataPoint, 0)
	}

	hdb.metrics[key] = append(hdb.metrics[key], dataPoint)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
