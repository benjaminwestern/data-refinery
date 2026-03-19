package processing

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/smtp"
	"sort"
	"strings"
	"sync"
	"time"
)

type (
	AlertingSeverity    string
	AlertingStatus      string
	NotificationChannel string
)

const (
	AlertingSeverityInfo     AlertingSeverity = "info"
	AlertingSeverityWarning  AlertingSeverity = "warning"
	AlertingSeverityError    AlertingSeverity = "error"
	AlertingSeverityCritical AlertingSeverity = "critical"
)

const (
	AlertingStatusActive   AlertingStatus = "active"
	AlertingStatusResolved AlertingStatus = "resolved"
	AlertingStatusSilenced AlertingStatus = "silenced"
)

const (
	ChannelEmail     NotificationChannel = "email"
	ChannelWebhook   NotificationChannel = "webhook"
	ChannelSlack     NotificationChannel = "slack"
	ChannelDiscord   NotificationChannel = "discord"
	ChannelSMS       NotificationChannel = "sms"
	ChannelPagerDuty NotificationChannel = "pagerduty"
)

type AlertingSystem struct {
	config        AlertingConfig
	rules         map[string]*AlertRule
	alerts        map[string]*Alert
	notifications map[string]*NotificationConfig
	escalations   map[string]*EscalationPolicy
	silences      map[string]*SilenceRule
	mutex         sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	channels      map[NotificationChannel]NotificationSender
	evaluator     *RuleEvaluator
	scheduler     *AlertScheduler
}

type AlertingConfig struct {
	EvaluationInterval  time.Duration `json:"evaluation_interval"`
	RetentionPeriod     time.Duration `json:"retention_period"`
	MaxAlerts           int           `json:"max_alerts"`
	EnableDeduplication bool          `json:"enable_deduplication"`
	EnableGrouping      bool          `json:"enable_grouping"`
	GroupingInterval    time.Duration `json:"grouping_interval"`
	EnableRateLimiting  bool          `json:"enable_rate_limiting"`
	RateLimit           int           `json:"rate_limit"`
	RateLimitWindow     time.Duration `json:"rate_limit_window"`
}

type AlertRule struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Query       string                 `json:"query"`
	Condition   AlertCondition         `json:"condition"`
	Threshold   float64                `json:"threshold"`
	Duration    time.Duration          `json:"duration"`
	Severity    AlertingSeverity       `json:"severity"`
	Labels      map[string]string      `json:"labels"`
	Annotations map[string]string      `json:"annotations"`
	Enabled     bool                   `json:"enabled"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	Metadata    map[string]interface{} `json:"metadata"`
}

type AlertCondition struct {
	Operator    string        `json:"operator"`
	Value       float64       `json:"value"`
	Aggregation string        `json:"aggregation"`
	Window      time.Duration `json:"window"`
}

type Alert struct {
	ID          string                 `json:"id"`
	RuleID      string                 `json:"rule_id"`
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Status      AlertingStatus         `json:"status"`
	Severity    AlertingSeverity       `json:"severity"`
	Value       float64                `json:"value"`
	Threshold   float64                `json:"threshold"`
	Labels      map[string]string      `json:"labels"`
	Annotations map[string]string      `json:"annotations"`
	StartsAt    time.Time              `json:"starts_at"`
	EndsAt      *time.Time             `json:"ends_at,omitempty"`
	UpdatedAt   time.Time              `json:"updated_at"`
	Count       int                    `json:"count"`
	Fingerprint string                 `json:"fingerprint"`
	Metadata    map[string]interface{} `json:"metadata"`
}

type NotificationConfig struct {
	ID       string                 `json:"id"`
	Name     string                 `json:"name"`
	Channel  NotificationChannel    `json:"channel"`
	Settings map[string]interface{} `json:"settings"`
	Filters  []AlertFilter          `json:"filters"`
	Enabled  bool                   `json:"enabled"`
}

type AlertFilter struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    string `json:"value"`
}

type EscalationPolicy struct {
	ID          string           `json:"id"`
	Name        string           `json:"name"`
	Steps       []EscalationStep `json:"steps"`
	Repeat      bool             `json:"repeat"`
	RepeatDelay time.Duration    `json:"repeat_delay"`
	Enabled     bool             `json:"enabled"`
}

type EscalationStep struct {
	Delay         time.Duration `json:"delay"`
	Notifications []string      `json:"notifications"`
}

type SilenceRule struct {
	ID        string           `json:"id"`
	Name      string           `json:"name"`
	Matchers  []SilenceMatcher `json:"matchers"`
	StartsAt  time.Time        `json:"starts_at"`
	EndsAt    time.Time        `json:"ends_at"`
	CreatedBy string           `json:"created_by"`
	Comment   string           `json:"comment"`
	Enabled   bool             `json:"enabled"`
}

type SilenceMatcher struct {
	Name    string `json:"name"`
	Value   string `json:"value"`
	IsRegex bool   `json:"is_regex"`
}

type NotificationSender interface {
	SendNotification(ctx context.Context, alert *Alert, config *NotificationConfig) error
	Name() string
}

type RuleEvaluator struct {
	alerting *AlertingSystem
}

type AlertScheduler struct {
	alerting *AlertingSystem
}

type EmailNotificationSender struct {
	smtpHost string
	smtpPort int
	username string
	password string
	useSSL   bool
	useTLS   bool
}

type WebhookNotificationSender struct {
	timeout time.Duration
}

type SlackNotificationSender struct {
	timeout time.Duration
}

func NewAlertingSystem(config AlertingConfig) *AlertingSystem {
	ctx, cancel := context.WithCancel(context.Background())

	alerting := &AlertingSystem{
		config:        config,
		rules:         make(map[string]*AlertRule),
		alerts:        make(map[string]*Alert),
		notifications: make(map[string]*NotificationConfig),
		escalations:   make(map[string]*EscalationPolicy),
		silences:      make(map[string]*SilenceRule),
		ctx:           ctx,
		cancel:        cancel,
		channels:      make(map[NotificationChannel]NotificationSender),
	}

	alerting.evaluator = &RuleEvaluator{alerting: alerting}
	alerting.scheduler = &AlertScheduler{alerting: alerting}

	alerting.setupNotificationChannels()

	return alerting
}

func (as *AlertingSystem) setupNotificationChannels() {
	as.channels[ChannelEmail] = &EmailNotificationSender{}
	as.channels[ChannelWebhook] = &WebhookNotificationSender{
		timeout: 30 * time.Second,
	}
	as.channels[ChannelSlack] = &SlackNotificationSender{
		timeout: 30 * time.Second,
	}
}

func (as *AlertingSystem) Start() error {
	go as.evaluateRules()
	go as.processAlerts()
	go as.cleanupOldAlerts()

	return nil
}

func (as *AlertingSystem) Stop() error {
	as.cancel()
	return nil
}

func (as *AlertingSystem) AddRule(rule *AlertRule) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	rule.CreatedAt = time.Now()
	rule.UpdatedAt = time.Now()

	as.rules[rule.ID] = rule
	return nil
}

func (as *AlertingSystem) UpdateRule(ruleID string, rule *AlertRule) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	existing, exists := as.rules[ruleID]
	if !exists {
		return fmt.Errorf("rule not found: %s", ruleID)
	}

	rule.ID = ruleID
	rule.CreatedAt = existing.CreatedAt
	rule.UpdatedAt = time.Now()

	as.rules[ruleID] = rule
	return nil
}

func (as *AlertingSystem) DeleteRule(ruleID string) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	delete(as.rules, ruleID)
	return nil
}

func (as *AlertingSystem) GetRule(ruleID string) (*AlertRule, bool) {
	as.mutex.RLock()
	defer as.mutex.RUnlock()

	rule, exists := as.rules[ruleID]
	return rule, exists
}

func (as *AlertingSystem) GetAllRules() map[string]*AlertRule {
	as.mutex.RLock()
	defer as.mutex.RUnlock()

	result := make(map[string]*AlertRule)
	for k, v := range as.rules {
		result[k] = v
	}
	return result
}

func (as *AlertingSystem) AddNotificationConfig(config *NotificationConfig) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	as.notifications[config.ID] = config
	return nil
}

func (as *AlertingSystem) AddEscalationPolicy(policy *EscalationPolicy) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	as.escalations[policy.ID] = policy
	return nil
}

func (as *AlertingSystem) AddSilenceRule(silence *SilenceRule) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	as.silences[silence.ID] = silence
	return nil
}

func (as *AlertingSystem) GetActiveAlerts() []*Alert {
	as.mutex.RLock()
	defer as.mutex.RUnlock()

	var activeAlerts []*Alert
	for _, alert := range as.alerts {
		if alert.Status == AlertingStatusActive {
			activeAlerts = append(activeAlerts, alert)
		}
	}

	sort.Slice(activeAlerts, func(i, j int) bool {
		return activeAlerts[i].StartsAt.After(activeAlerts[j].StartsAt)
	})

	return activeAlerts
}

func (as *AlertingSystem) GetAlert(alertID string) (*Alert, bool) {
	as.mutex.RLock()
	defer as.mutex.RUnlock()

	alert, exists := as.alerts[alertID]
	return alert, exists
}

func (as *AlertingSystem) SilenceAlert(alertID string, duration time.Duration, comment string) error {
	as.mutex.Lock()
	defer as.mutex.Unlock()

	alert, exists := as.alerts[alertID]
	if !exists {
		return fmt.Errorf("alert not found: %s", alertID)
	}

	alert.Status = AlertingStatusSilenced
	alert.UpdatedAt = time.Now()

	silenceRule := &SilenceRule{
		ID:       fmt.Sprintf("silence_%s_%d", alertID, time.Now().Unix()),
		Name:     fmt.Sprintf("Silence for alert %s", alertID),
		StartsAt: time.Now(),
		EndsAt:   time.Now().Add(duration),
		Comment:  comment,
		Enabled:  true,
		Matchers: []SilenceMatcher{
			{
				Name:    "alert_id",
				Value:   alertID,
				IsRegex: false,
			},
		},
	}

	as.silences[silenceRule.ID] = silenceRule
	return nil
}

func (as *AlertingSystem) evaluateRules() {
	ticker := time.NewTicker(as.config.EvaluationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-as.ctx.Done():
			return
		case <-ticker.C:
			as.evaluator.EvaluateAllRules()
		}
	}
}

func (as *AlertingSystem) processAlerts() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-as.ctx.Done():
			return
		case <-ticker.C:
			as.scheduler.ProcessAlerts()
		}
	}
}

func (as *AlertingSystem) cleanupOldAlerts() {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-as.ctx.Done():
			return
		case <-ticker.C:
			as.mutex.Lock()
			cutoff := time.Now().Add(-as.config.RetentionPeriod)

			for alertID, alert := range as.alerts {
				if alert.Status == AlertingStatusResolved && alert.EndsAt != nil && alert.EndsAt.Before(cutoff) {
					delete(as.alerts, alertID)
				}
			}
			as.mutex.Unlock()
		}
	}
}

func (re *RuleEvaluator) EvaluateAllRules() {
	re.alerting.mutex.RLock()
	rules := make(map[string]*AlertRule)
	for k, v := range re.alerting.rules {
		if v.Enabled {
			rules[k] = v
		}
	}
	re.alerting.mutex.RUnlock()

	for _, rule := range rules {
		go re.EvaluateRule(rule)
	}
}

func (re *RuleEvaluator) EvaluateRule(rule *AlertRule) {
	result := re.executeQuery(rule.Query)

	shouldAlert := false
	switch rule.Condition.Operator {
	case ">":
		shouldAlert = result > rule.Threshold
	case "<":
		shouldAlert = result < rule.Threshold
	case ">=":
		shouldAlert = result >= rule.Threshold
	case "<=":
		shouldAlert = result <= rule.Threshold
	case "==":
		shouldAlert = result == rule.Threshold
	case "!=":
		shouldAlert = result != rule.Threshold
	}

	fingerprint := re.generateFingerprint(rule, result)

	re.alerting.mutex.Lock()
	defer re.alerting.mutex.Unlock()

	existingAlert, exists := re.alerting.alerts[fingerprint]

	if shouldAlert {
		if !exists {
			alert := &Alert{
				ID:          fingerprint,
				RuleID:      rule.ID,
				Name:        rule.Name,
				Description: rule.Description,
				Status:      AlertingStatusActive,
				Severity:    rule.Severity,
				Value:       result,
				Threshold:   rule.Threshold,
				Labels:      rule.Labels,
				Annotations: rule.Annotations,
				StartsAt:    time.Now(),
				UpdatedAt:   time.Now(),
				Count:       1,
				Fingerprint: fingerprint,
				Metadata:    make(map[string]interface{}),
			}

			re.alerting.alerts[fingerprint] = alert
			log.Printf("New alert: %s - %s", alert.Name, alert.Description)
		} else {
			existingAlert.Value = result
			existingAlert.Count++
			existingAlert.UpdatedAt = time.Now()

			if existingAlert.Status != AlertingStatusActive {
				existingAlert.Status = AlertingStatusActive
				existingAlert.StartsAt = time.Now()
				existingAlert.EndsAt = nil
				log.Printf("Alert reactivated: %s", existingAlert.Name)
			}
		}
	} else if exists && existingAlert.Status == AlertingStatusActive {
		existingAlert.Status = AlertingStatusResolved
		now := time.Now()
		existingAlert.EndsAt = &now
		existingAlert.UpdatedAt = now
		log.Printf("Alert resolved: %s", existingAlert.Name)
	}
}

func (re *RuleEvaluator) executeQuery(query string) float64 {
	return 0.0
}

func (re *RuleEvaluator) generateFingerprint(rule *AlertRule, value float64) string {
	labelStr := ""
	if rule.Labels != nil {
		var labels []string
		for k, v := range rule.Labels {
			labels = append(labels, fmt.Sprintf("%s=%s", k, v))
		}
		sort.Strings(labels)
		labelStr = strings.Join(labels, ",")
	}

	return fmt.Sprintf("%s_%s_%f", rule.ID, labelStr, value)
}

func (as *AlertScheduler) ProcessAlerts() {
	as.alerting.mutex.RLock()
	alerts := make([]*Alert, 0)
	for _, alert := range as.alerting.alerts {
		if alert.Status == AlertingStatusActive && !as.isAlertSilenced(alert) {
			alerts = append(alerts, alert)
		}
	}
	as.alerting.mutex.RUnlock()

	for _, alert := range alerts {
		go as.processAlert(alert)
	}
}

func (as *AlertScheduler) processAlert(alert *Alert) {
	as.alerting.mutex.RLock()
	notifications := make([]*NotificationConfig, 0)
	for _, config := range as.alerting.notifications {
		if config.Enabled && as.matchesFilter(alert, config.Filters) {
			notifications = append(notifications, config)
		}
	}
	as.alerting.mutex.RUnlock()

	for _, config := range notifications {
		go as.sendNotification(alert, config)
	}
}

func (as *AlertScheduler) sendNotification(alert *Alert, config *NotificationConfig) {
	sender, exists := as.alerting.channels[config.Channel]
	if !exists {
		log.Printf("Notification channel not found: %s", config.Channel)
		return
	}

	ctx, cancel := context.WithTimeout(as.alerting.ctx, 30*time.Second)
	defer cancel()

	if err := sender.SendNotification(ctx, alert, config); err != nil {
		log.Printf("Failed to send notification: %v", err)
	}
}

func (as *AlertScheduler) isAlertSilenced(alert *Alert) bool {
	as.alerting.mutex.RLock()
	defer as.alerting.mutex.RUnlock()

	now := time.Now()
	for _, silence := range as.alerting.silences {
		if silence.Enabled && silence.StartsAt.Before(now) && silence.EndsAt.After(now) {
			if as.matchesSilence(alert, silence) {
				return true
			}
		}
	}
	return false
}

func (as *AlertScheduler) matchesSilence(alert *Alert, silence *SilenceRule) bool {
	for _, matcher := range silence.Matchers {
		if matcher.Name == "alert_id" && matcher.Value == alert.ID {
			return true
		}

		if labelValue, exists := alert.Labels[matcher.Name]; exists {
			if matcher.IsRegex {
				return true
			} else if labelValue == matcher.Value {
				return true
			}
		}
	}
	return false
}

func (as *AlertScheduler) matchesFilter(alert *Alert, filters []AlertFilter) bool {
	if len(filters) == 0 {
		return true
	}

	for _, filter := range filters {
		switch filter.Field {
		case "severity":
			if string(alert.Severity) != filter.Value {
				return false
			}
		case "status":
			if string(alert.Status) != filter.Value {
				return false
			}
		}
	}

	return true
}

func (ens *EmailNotificationSender) SendNotification(ctx context.Context, alert *Alert, config *NotificationConfig) error {
	to, ok := config.Settings["to"].(string)
	if !ok {
		return fmt.Errorf("email 'to' address not configured")
	}

	from, ok := config.Settings["from"].(string)
	if !ok {
		return fmt.Errorf("email 'from' address not configured")
	}

	smtpHost, ok := config.Settings["smtp_host"].(string)
	if !ok {
		return fmt.Errorf("SMTP host not configured")
	}

	smtpPort, ok := config.Settings["smtp_port"].(int)
	if !ok {
		smtpPort = 587
	}

	username, _ := config.Settings["username"].(string)
	password, _ := config.Settings["password"].(string)

	subject := fmt.Sprintf("[%s] %s", strings.ToUpper(string(alert.Severity)), alert.Name)
	body := fmt.Sprintf("Alert: %s\n\nDescription: %s\n\nSeverity: %s\nValue: %.2f\nThreshold: %.2f\nStarted: %s\n\nLabels: %v\n",
		alert.Name, alert.Description, alert.Severity, alert.Value, alert.Threshold, alert.StartsAt.Format(time.RFC3339), alert.Labels)

	message := fmt.Sprintf("From: %s\nTo: %s\nSubject: %s\n\n%s", from, to, subject, body)

	auth := smtp.PlainAuth("", username, password, smtpHost)

	addr := fmt.Sprintf("%s:%d", smtpHost, smtpPort)

	return smtp.SendMail(addr, auth, from, []string{to}, []byte(message))
}

func (ens *EmailNotificationSender) Name() string {
	return "Email"
}

func (wns *WebhookNotificationSender) SendNotification(ctx context.Context, alert *Alert, config *NotificationConfig) error {
	url, ok := config.Settings["url"].(string)
	if !ok {
		return fmt.Errorf("webhook URL not configured")
	}

	payload := map[string]interface{}{
		"alert":     alert,
		"config":    config,
		"timestamp": time.Now().Format(time.RFC3339),
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook payload: %v", err)
	}

	client := &http.Client{
		Timeout: wns.timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: false},
			DialContext: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).DialContext,
		},
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(jsonPayload)))
	if err != nil {
		return fmt.Errorf("failed to create webhook request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "AlertingSystem/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send webhook: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("webhook returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (wns *WebhookNotificationSender) Name() string {
	return "Webhook"
}

func (sns *SlackNotificationSender) SendNotification(ctx context.Context, alert *Alert, config *NotificationConfig) error {
	webhookURL, ok := config.Settings["webhook_url"].(string)
	if !ok {
		return fmt.Errorf("slack webhook URL not configured")
	}

	color := "good"
	switch alert.Severity {
	case AlertingSeverityWarning:
		color = "warning"
	case AlertingSeverityError, AlertingSeverityCritical:
		color = "danger"
	}

	payload := map[string]interface{}{
		"text": fmt.Sprintf("Alert: %s", alert.Name),
		"attachments": []map[string]interface{}{
			{
				"color": color,
				"title": alert.Name,
				"text":  alert.Description,
				"fields": []map[string]interface{}{
					{
						"title": "Severity",
						"value": string(alert.Severity),
						"short": true,
					},
					{
						"title": "Value",
						"value": fmt.Sprintf("%.2f", alert.Value),
						"short": true,
					},
					{
						"title": "Threshold",
						"value": fmt.Sprintf("%.2f", alert.Threshold),
						"short": true,
					},
					{
						"title": "Started",
						"value": alert.StartsAt.Format(time.RFC3339),
						"short": true,
					},
				},
				"timestamp": alert.StartsAt.Unix(),
			},
		},
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal slack payload: %v", err)
	}

	client := &http.Client{Timeout: sns.timeout}
	resp, err := client.Post(webhookURL, "application/json", strings.NewReader(string(jsonPayload)))
	if err != nil {
		return fmt.Errorf("failed to send slack notification: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("slack returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (sns *SlackNotificationSender) Name() string {
	return "Slack"
}
