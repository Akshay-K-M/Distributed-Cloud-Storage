// This file runs periodic checksum scans and quarantines corrupt chunks.
package worker

import (
	"context"
	"log/slog"
	"time"

	"objstore/internal/worker/chunkstore"
)

// Scrubber periodically rehashes on-disk chunks.
type Scrubber struct {
	store    *chunkstore.Store
	interval time.Duration
	rateBPS  int64
	log      *slog.Logger
	trigger  chan struct{}
}

// NewScrubber builds a scrubber. rateMBPS is converted to bytes/sec.
func NewScrubber(store *chunkstore.Store, interval time.Duration, rateMBPS int, log *slog.Logger) *Scrubber {
	return &Scrubber{
		store:    store,
		interval: interval,
		rateBPS:  int64(rateMBPS) * 1024 * 1024,
		log:      log,
		trigger:  make(chan struct{}, 1),
	}
}

// Trigger requests an immediate scrub (POST /internal/scrub).
func (s *Scrubber) Trigger() {
	select {
	case s.trigger <- struct{}{}:
	default:
	}
}

// Run blocks until ctx is cancelled.
func (s *Scrubber) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.runOnce(ctx)
		case <-s.trigger:
			s.runOnce(ctx)
		}
	}
}

func (s *Scrubber) runOnce(ctx context.Context) {
	s.log.Info("scrub start")
	bad := s.store.Scrub(ctx, s.rateBPS)
	if len(bad) > 0 {
		s.log.Warn("scrub quarantined chunks", "ids", bad)
	}
	s.log.Info("scrub done", "quarantined", len(bad))
}
