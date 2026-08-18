// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	// The INSERT is the dedup gate: concurrent or repeated redeliveries of
	// the same event_id conflict on the unique index and count nothing.
	inserted, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		go func() {
			if err := s.processRecording(context.Background(), rec.CallID); err != nil {
				s.log.Error("recording processing failed", "call_id", rec.CallID, "err", err)
			}
		}()
	}

	return nil
}

// HydrateFromStore rebuilds the in-memory cache from the durable
// aggregates so a freshly deployed process serves correct numbers
// immediately instead of starting from zero.
func (s *Service) HydrateFromStore(ctx context.Context) error {
	durable, err := s.store.AllAccountStats(ctx)
	if err != nil {
		return err
	}
	for accountID, st := range durable {
		s.cache.Put(accountID, stats.AccountStats{
			CallCount:        st.CallCount,
			TotalDurationSec: st.TotalDurationSec,
		})
	}
	return nil
}

// ReplayPendingRecordings resumes recording processing for calls left
// unprocessed by an earlier process, so work in flight at deploy time is
// not lost. Marking is idempotent, so overlapping replays are safe.
func (s *Service) ReplayPendingRecordings(ctx context.Context) error {
	pending, err := s.store.PendingRecordings(ctx)
	if err != nil {
		return err
	}
	for _, callID := range pending {
		if err := s.processRecording(ctx, callID); err != nil {
			s.log.Error("recording replay failed", "call_id", callID, "err", err)
		}
	}
	return nil
}

// Restore is the startup sequence after a deploy: hydrate the cache from
// the durable copy, then resume unfinished recording work.
func (s *Service) Restore(ctx context.Context) error {
	if err := s.HydrateFromStore(ctx); err != nil {
		return err
	}
	return s.ReplayPendingRecordings(ctx)
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, callID string) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, callID)
}
