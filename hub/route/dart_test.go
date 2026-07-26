package route

import (
	"bytes"
	"encoding/json"
	"strconv"
	"testing"
	"time"

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

func TestDartDialFeedbackSignalsAreOptIn(t *testing.T) {
	start := dialfeedback.Default.SnapshotSince(0).Sequence
	dialfeedback.Default.AddSupplementalSignal("smart", "node", "tcp", "tcp", true, time.Millisecond, "")
	dialfeedback.Default.AddLegacySignalDurations(
		"smart",
		"node",
		"tcp",
		"handshake",
		true,
		2*time.Millisecond,
		102*time.Millisecond,
		"",
	)
	dialfeedback.Default.AddSupplementalSignal(
		"smart",
		"node",
		"tcp",
		"first-byte",
		false,
		3*time.Millisecond,
		"private.example:443",
	)

	requestSnapshot := func(suffix string) dialfeedback.Snapshot {
		t.Helper()
		request := httptest.NewRequest(
			http.MethodGet,
			"/dial-feedback?since="+strconv.FormatUint(start, 10)+suffix,
			http.NoBody,
		)
		recorder := httptest.NewRecorder()
		dartRouter().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
		}
		if bytes.Contains(recorder.Body.Bytes(), []byte("private.example")) ||
			bytes.Contains(recorder.Body.Bytes(), []byte(`"legacy"`)) {
			t.Fatalf("response leaked private/internal data: %s", recorder.Body.Bytes())
		}
		var snapshot dialfeedback.Snapshot
		if err := json.Unmarshal(recorder.Body.Bytes(), &snapshot); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		return snapshot
	}

	legacy := requestSnapshot("")
	if len(legacy.Events) != 1 || legacy.Events[0].Signal != "handshake" {
		t.Fatalf("legacy response changed: %#v", legacy)
	}
	if legacy.Events[0].DurationMs != 102 {
		t.Fatalf("legacy duration=%d want 102", legacy.Events[0].DurationMs)
	}
	nonOptIn := requestSnapshot("&signals=true")
	if len(nonOptIn.Events) != 1 || nonOptIn.Events[0].Signal != "handshake" {
		t.Fatalf("non-exact opt-in exposed staged signals: %#v", nonOptIn)
	}
	full := requestSnapshot("&signals=1")
	if full.Sequence != legacy.Sequence || len(full.Events) != 3 {
		t.Fatalf("full response=%#v legacy=%#v", full, legacy)
	}
	if full.Events[1].DurationMs != 2 {
		t.Fatalf("detailed handshake duration=%d want 2", full.Events[1].DurationMs)
	}
	signals := []string{full.Events[0].Signal, full.Events[1].Signal, full.Events[2].Signal}
	if signals[0] != "tcp" || signals[1] != "handshake" || signals[2] != "first-byte" {
		t.Fatalf("signals=%v", signals)
	}
}
