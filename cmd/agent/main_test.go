package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func get(t *testing.T, handler http.Handler, path string) int {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	return recorder.Code
}

func TestHealth(t *testing.T) {
	h := &health{started: time.Now(), stale: time.Minute}
	handler := h.routes(prometheus.NewRegistry())
	if code := get(t, handler, "/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz before any reconciliation = %d, want 503", code)
	}
	if code := get(t, handler, "/healthz"); code != http.StatusOK {
		t.Errorf("/healthz just after starting = %d, want 200", code)
	}
	h.succeeded()
	if code := get(t, handler, "/readyz"); code != http.StatusOK {
		t.Errorf("/readyz after a reconciliation = %d, want 200", code)
	}
	h.lastSuccess.Store(time.Now().Add(-2 * time.Minute).UnixNano())
	if code := get(t, handler, "/healthz"); code != http.StatusServiceUnavailable {
		t.Errorf("/healthz with a stale last success = %d, want 503", code)
	}
	if code := get(t, handler, "/metrics"); code != http.StatusOK {
		t.Errorf("/metrics = %d, want 200", code)
	}
}

func TestRunNeedsBootstrap(t *testing.T) {
	var stderr bytes.Buffer
	if err := run(t.Context(), nil, &stderr); err == nil || !strings.Contains(err.Error(), "-bootstrap") {
		t.Errorf("run() = %v, want an error about -bootstrap", err)
	}
}
