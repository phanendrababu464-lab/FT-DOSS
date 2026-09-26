package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ft-doss/backend/internal/observability"
	"github.com/ft-doss/backend/pkg/logger"
	"github.com/stretchr/testify/assert"
)

func TestSSEAuth_UnauthorizedRejection(t *testing.T) {
	log := logger.New("test", "info", "json")
	alerts := observability.NewAlertManager()

	cfg := &GatewayConfig{
		WriteQuorum: 2,
		ReadQuorum:  2,
		AdminAPIKey: "secret-admin-key",
	}

	gw := NewGateway(nil, nil, nil, nil, nil, nil, alerts, cfg, log)
	router := gw.Router()

	// Test 1: GET /api/events/stream without token or X-Admin-API-Key header -> 401
	req, _ := http.NewRequest("GET", "/api/events/stream", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestSSEAuth_TicketGenerationAndConsumption(t *testing.T) {
	log := logger.New("test", "info", "json")
	alerts := observability.NewAlertManager()

	cfg := &GatewayConfig{
		WriteQuorum: 2,
		ReadQuorum:  2,
		AdminAPIKey: "secret-admin-key",
	}

	gw := NewGateway(nil, nil, nil, nil, nil, nil, alerts, cfg, log)
	router := gw.Router()

	// Test 1: Fetch SSE ticket with valid admin key header -> 200 OK
	reqToken, _ := http.NewRequest("POST", "/admin/sse-token", nil)
	reqToken.Header.Set("X-Admin-API-Key", "secret-admin-key")
	wToken := httptest.NewRecorder()
	router.ServeHTTP(wToken, reqToken)

	assert.Equal(t, http.StatusOK, wToken.Code)
	assert.Contains(t, wToken.Body.String(), "token")
}
