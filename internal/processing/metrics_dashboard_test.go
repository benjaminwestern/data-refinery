package processing

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSetupHTTPServerDefaultsToLoopbackAndTimeouts(t *testing.T) {
	dashboard := NewMetricsDashboard(DashboardConfig{
		EnableHTTPAPI: true,
		HTTPAPIPort:   8080,
	})

	if dashboard.httpServer == nil {
		t.Fatal("expected HTTP server to be configured")
	}

	if got, want := dashboard.httpServer.Addr, "127.0.0.1:8080"; got != want {
		t.Fatalf("expected bind address %q, got %q", want, got)
	}

	if dashboard.httpServer.ReadHeaderTimeout != defaultHTTPReadHeaderTime {
		t.Fatalf("expected ReadHeaderTimeout %v, got %v", defaultHTTPReadHeaderTime, dashboard.httpServer.ReadHeaderTimeout)
	}
	if dashboard.httpServer.ReadTimeout != defaultHTTPReadTimeout {
		t.Fatalf("expected ReadTimeout %v, got %v", defaultHTTPReadTimeout, dashboard.httpServer.ReadTimeout)
	}
	if dashboard.httpServer.WriteTimeout != defaultHTTPWriteTimeout {
		t.Fatalf("expected WriteTimeout %v, got %v", defaultHTTPWriteTimeout, dashboard.httpServer.WriteTimeout)
	}
	if dashboard.httpServer.IdleTimeout != defaultHTTPIdleTimeout {
		t.Fatalf("expected IdleTimeout %v, got %v", defaultHTTPIdleTimeout, dashboard.httpServer.IdleTimeout)
	}
	if dashboard.httpServer.MaxHeaderBytes != defaultHTTPMaxHeaderBytes {
		t.Fatalf("expected MaxHeaderBytes %d, got %d", defaultHTTPMaxHeaderBytes, dashboard.httpServer.MaxHeaderBytes)
	}
}

func TestSetupHTTPServerRejectsRemoteBindWithoutAuthToken(t *testing.T) {
	dashboard := NewMetricsDashboard(DashboardConfig{
		EnableHTTPAPI:      true,
		HTTPAPIPort:        9090,
		HTTPAPIBindAddress: "0.0.0.0",
	})

	if got, want := dashboard.httpServer.Addr, "127.0.0.1:9090"; got != want {
		t.Fatalf("expected loopback fallback %q, got %q", want, got)
	}
}

func TestSetupHTTPServerAllowsRemoteBindWithAuthToken(t *testing.T) {
	dashboard := NewMetricsDashboard(DashboardConfig{
		EnableHTTPAPI:      true,
		HTTPAPIPort:        9090,
		HTTPAPIBindAddress: "0.0.0.0",
		HTTPAPIAuthToken:   "secret-token",
	})

	if got, want := dashboard.httpServer.Addr, "0.0.0.0:9090"; got != want {
		t.Fatalf("expected authenticated remote bind %q, got %q", want, got)
	}

	unauthorizedRequest := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	unauthorizedResponse := httptest.NewRecorder()
	dashboard.httpServer.Handler.ServeHTTP(unauthorizedResponse, unauthorizedRequest)

	if unauthorizedResponse.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized status %d, got %d", http.StatusUnauthorized, unauthorizedResponse.Code)
	}

	authorizedRequest := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	authorizedRequest.Header.Set("Authorization", "Bearer secret-token")
	authorizedResponse := httptest.NewRecorder()
	dashboard.httpServer.Handler.ServeHTTP(authorizedResponse, authorizedRequest)

	if authorizedResponse.Code != http.StatusOK {
		t.Fatalf("expected success status %d, got %d", http.StatusOK, authorizedResponse.Code)
	}
}
