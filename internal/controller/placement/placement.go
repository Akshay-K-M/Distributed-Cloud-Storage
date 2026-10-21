package placement

import (
	"errors"
	"math/rand"
	"sort"
	"sync"

	"objstore/internal/controller/registry"
)

var ErrInsufficientWorkers = errors.New("insufficient workers")

// Placer selects worker nodes for chunk placement according to the least-loaded policy.
type Placer struct {
	mu  sync.Mutex
	reg *registry.Registry
}

// New creates a new Placer.
func New(reg *registry.Registry) *Placer {
	return &Placer{reg: reg}
}

// Pick selects n distinct ALIVE workers with sufficient capacity, sorted ascending by UsedBytes.
// It optimistically increases the UsedBytes on the selected workers.
func (p *Placer) Pick(n int, exclude map[string]bool, size int64) ([]registry.WorkerInfo, error) {
	if n <= 0 {
		return nil, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	alive := p.reg.Alive()

	type candidate struct {
		info    registry.WorkerInfo
		randKey int
	}

	var candidates []candidate
	for _, w := range alive {
		if exclude != nil && exclude[w.ID] {
			continue
		}
		// capacity 0 = unknown = allowed
		if w.Capacity > 0 && (w.Capacity-w.UsedBytes) < size {
			continue
		}
		candidates = append(candidates, candidate{
			info:    w,
			randKey: rand.Int(),
		})
	}

	if len(candidates) < n {
		return nil, ErrInsufficientWorkers
	}

	// Sort ascending by UsedBytes with random tiebreak
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].info.UsedBytes != candidates[j].info.UsedBytes {
			return candidates[i].info.UsedBytes < candidates[j].info.UsedBytes
		}
		return candidates[i].randKey < candidates[j].randKey
	})

	picked := make([]registry.WorkerInfo, n)
	for i := 0; i < n; i++ {
		w := candidates[i].info
		picked[i] = w
		// Optimistically add size to UsedBytes
		p.reg.AddUsed(w.ID, size)
	}

	return picked, nil
}

// Release rolls back the optimistic UsedBytes addition if a chunk write fails.
func (p *Placer) Release(workerID string, size int64) {
	p.reg.AddUsed(workerID, -size)
}
