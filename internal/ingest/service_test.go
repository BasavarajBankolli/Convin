package ingest_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

func TestRecordingGetsMarkedProcessed(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		var processed bool
		row := st.Pool().QueryRow(ctx,
			`SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
		if err := row.Scan(&processed); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if processed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("recording was never marked processed")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestConcurrentDuplicateDeliveryCountsOnce(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	const deliveries = 20
	var wg sync.WaitGroup
	for i := 0; i < deliveries; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", strings.NewReader(body))
			if err != nil {
				t.Errorf("delivery: %v", err)
				return
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("delivery got %d, want 200", resp.StatusCode)
			}
		}()
	}
	wg.Wait()

	var events, statsCount int64
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	row = st.Pool().QueryRow(ctx, `SELECT call_count FROM account_stats WHERE account_id = $1`, accountID)
	if err := row.Scan(&statsCount); err != nil {
		t.Fatalf("count stats: %v", err)
	}

	var calls int64
	row = st.Pool().QueryRow(ctx, `SELECT count(*) FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&calls); err != nil {
		t.Fatalf("count calls: %v", err)
	}

	if events != 1 || statsCount != 1 || calls != 1 {
		t.Fatalf("got events=%d stats=%d calls=%d, want 1/1/1 (concurrent redelivery double-counted)", events, statsCount, calls)
	}
}

func TestStatsSurviveRestart(t *testing.T) {
	// "First boot": ingest one call into the database.
	srv1, st := testutil.NewServer(t)
	_, _, accountID := testutil.IDs(t, st)

	body := eventJSON("evt_restart", "call_restart", accountID)
	if resp := post(t, srv1.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}
	srv1.Close() // the in-memory cache dies with the process

	// "Deploy": a fresh process, cold cache, same database.
	srv2, _ := testutil.NewServer(t)
	resp, err := http.Get(srv2.URL + "/accounts/" + accountID + "/stats")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["call_count"].(float64) != 1 || got["total_duration_sec"].(float64) != 143 {
		t.Fatalf("stats after restart: got %v, want call_count=1 total_duration_sec=143", got)
	}
}

func TestPendingRecordingIsReplayedAfterRestart(t *testing.T) {
	st := testutil.NewStore(t)
	_, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	// A call whose recording was still in flight when the process died:
	// the row is durable, the processing marker is not.
	if _, err := st.Pool().Exec(ctx,
		`INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url)
		 VALUES ($1, $2, 'completed', 143, 'https://recordings.example.com/' || $3 || '.wav')`,
		callID, accountID, callID); err != nil {
		t.Fatalf("seed call: %v", err)
	}

	// The deploy: a fresh process restores its state from the database.
	testutil.NewService(t, st)

	deadline := time.Now().Add(2 * time.Second)
	for {
		var processed bool
		row := st.Pool().QueryRow(ctx,
			`SELECT recording_processed FROM calls WHERE call_id = $1`, callID)
		if err := row.Scan(&processed); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if processed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("recording in flight at restart was never picked up")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
