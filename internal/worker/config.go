// This file loads and validates worker runtime configuration.
package worker

import (
	"fmt"
	"time"

	"objstore/internal/common/config"
	"objstore/internal/worker/chunkstore"
)

// Config is worker process configuration.
type Config struct {
	Token             string
	WorkerID          string
	WorkerAddr        string
	Listen            string
	ControllerURL     string
	DataDir           string
	HeartbeatInterval time.Duration
	ScrubInterval     time.Duration
	ScrubRateMBPS     int
	ChunkSize         int64
}

// LoadConfig reads worker env vars and fail-fast validates.
func LoadConfig() (Config, error) {
	var c Config
	var err error
	if c.Token, err = config.MustEnv("CLUSTER_TOKEN"); err != nil {
		return c, err
	}
	if c.WorkerID, err = config.MustEnv("WORKER_ID"); err != nil {
		return c, err
	}
	if c.WorkerAddr, err = config.MustEnv("WORKER_ADDR"); err != nil {
		return c, err
	}
	c.Listen = config.Env("WORKER_LISTEN", ":9000")
	c.ControllerURL = config.Env("CONTROLLER_URL", "http://controller:8080")
	c.DataDir = config.Env("DATA_DIR", "/data")
	if c.HeartbeatInterval, err = config.EnvDuration("HEARTBEAT_INTERVAL", 2*time.Second); err != nil {
		return c, err
	}
	if c.ScrubInterval, err = config.EnvDuration("SCRUB_INTERVAL", 10*time.Minute); err != nil {
		return c, err
	}
	if c.ScrubRateMBPS, err = config.EnvInt("SCRUB_RATE_MBPS", 50); err != nil {
		return c, err
	}
	if c.ChunkSize, err = config.EnvInt64("CHUNK_SIZE", chunkstore.DefaultMaxChunk); err != nil {
		return c, err
	}
	if c.HeartbeatInterval <= 0 {
		return c, fmt.Errorf("HEARTBEAT_INTERVAL must be > 0")
	}
	if c.ScrubInterval <= 0 {
		return c, fmt.Errorf("SCRUB_INTERVAL must be > 0")
	}
	if c.ScrubRateMBPS < 0 {
		return c, fmt.Errorf("SCRUB_RATE_MBPS must be >= 0")
	}
	if c.ChunkSize <= 0 {
		return c, fmt.Errorf("CHUNK_SIZE must be > 0")
	}
	return c, nil
}
