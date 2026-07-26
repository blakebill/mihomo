package route

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/metacubex/mihomo/common/dialfeedback"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
)

func TestDartDialFeedbackIsIncrementalAndNotCached(t *testing.T) {
	snapshot := dialfeedback.Default.SnapshotSince(0)
	request := httptest.NewRequest(
		http.MethodGet,
		"/dial-feedback?since="+strconv.FormatUint(snapshot.Sequence, 10),
		http.NoBody,
	)
	recorder := httptest.NewRecorder()

	dartRouter().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if cacheControl := recorder.Header().Get("Cache-Control"); cacheControl != "no-store" {
		t.Fatalf("cache-control=%q", cacheControl)
	}
	var response dialfeedback.Snapshot
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Instance != snapshot.Instance || len(response.Instance) != 32 ||
		response.Sequence != snapshot.Sequence || response.Events == nil || len(response.Events) != 0 {
		t.Fatalf("unexpected response: %#v", response)
	}
}

func TestDartDialFeedbackRejectsInvalidCursor(t *testing.T) {
	request := httptest.NewRequest(
		http.MethodGet,
		"/dial-feedback?since=invalid",
		http.NoBody,
	)
	recorder := httptest.NewRecorder()

	dartRouter().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if cacheControl := recorder.Header().Get("Cache-Control"); cacheControl != "no-store" {
		t.Fatalf("cache-control=%q", cacheControl)
	}
}
