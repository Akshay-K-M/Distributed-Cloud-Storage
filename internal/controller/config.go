// This file loads and validates controller runtime configuration.
package controller

import (
	"fmt"
	"time"

	"objstore/internal/common/config"
)

// Config holds all controller configuration parameters.
type Config struct {
	ClusterToken          string
	ListenAddr            string
	DBPath                string
	ChunkSize             int64
	ReplicationFactor     int
	WriteAcks             int
	UploadParallelism     int
	MaxConcurrentUploads  int
	MaxObjectBytes        int64
	HeartbeatInterval     time.Duration
	SuspectAfter          time.Duration
	DeadAfter             time.Duration
	RepairEnabled         bool
	RepairDelay           time.Duration
	StartupGrace          time.Duration
	ReconcileInterval     time.Duration
	InventorySyncInterval time.Duration
	RepairConcurrency     int
}

// LoadConfig loads and validates controller configuration from environment variables.
func LoadConfig() (Config, error) {
	token, err := config.MustEnv("CLUSTER_TOKEN")
	if err != nil {
		return Config{}, err
	}

	listen := config.Env("LISTEN_ADDR", ":8080")
	dbPath := config.Env("DB_PATH", "/data/meta.db")

	chunkSize, err := config.EnvInt64("CHUNK_SIZE", 1048576)
	if err != nil {
		return Config{}, err
	}

	rf, err := config.EnvInt("REPLICATION_FACTOR", 2)
	if err != nil {
		return Config{}, err
	}

	wAcks, err := config.EnvInt("WRITE_ACKS", 2)
	if err != nil {
		return Config{}, err
	}

	uploadPar, err := config.EnvInt("UPLOAD_PARALLELISM", 4)
	if err != nil {
		return Config{}, err
	}

	maxUploads, err := config.EnvInt("MAX_CONCURRENT_UPLOADS", 16)
	if err != nil {
		return Config{}, err
	}

	maxObjBytes, err := config.EnvInt64("MAX_OBJECT_BYTES", 5368709120) // 5 GiB
	if err != nil {
		return Config{}, err
	}

	hbInterval, err := config.EnvDuration("HEARTBEAT_INTERVAL", 2*time.Second)
	if err != nil {
		return Config{}, err
	}

	suspectAfter, err := config.EnvDuration("SUSPECT_AFTER", 6*time.Second)
	if err != nil {
		return Config{}, err
	}

	deadAfter, err := config.EnvDuration("DEAD_AFTER", 15*time.Second)
	if err != nil {
		return Config{}, err
	}

	repairEnabled, err := config.EnvBool("REPAIR_ENABLED", true)
	if err != nil {
		return Config{}, err
	}

	repairDelay, err := config.EnvDuration("REPAIR_DELAY", 5*time.Second)
	if err != nil {
		return Config{}, err
	}

	startupGrace, err := config.EnvDuration("STARTUP_GRACE", 15*time.Second)
	if err != nil {
		return Config{}, err
	}

	reconcileInterval, err := config.EnvDuration("RECONCILE_INTERVAL", 5*time.Second)
	if err != nil {
		return Config{}, err
	}

	invInterval, err := config.EnvDuration("INVENTORY_SYNC_INTERVAL", 5*time.Minute)
	if err != nil {
		return Config{}, err
	}

	repairConc, err := config.EnvInt("REPAIR_CONCURRENCY", 2)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{
		ClusterToken:          token,
		ListenAddr:            listen,
		DBPath:                dbPath,
		ChunkSize:             chunkSize,
		ReplicationFactor:     rf,
		WriteAcks:             wAcks,
		UploadParallelism:     uploadPar,
		MaxConcurrentUploads:  maxUploads,
		MaxObjectBytes:        maxObjBytes,
		HeartbeatInterval:     hbInterval,
		SuspectAfter:          suspectAfter,
		DeadAfter:             deadAfter,
		RepairEnabled:         repairEnabled,
		RepairDelay:           repairDelay,
		StartupGrace:          startupGrace,
		ReconcileInterval:     reconcileInterval,
		InventorySyncInterval: invInterval,
		RepairConcurrency:     repairConc,
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

// Validate checks all configuration constraints and fails fast with a clear message.
func (c *Config) Validate() error {
	if c.ClusterToken == "" {
		return fmt.Errorf("CLUSTER_TOKEN is required")
	}
	if c.ChunkSize <= 0 {
		return fmt.Errorf("CHUNK_SIZE must be > 0, got %d", c.ChunkSize)
	}
	if c.ReplicationFactor < 1 {
		return fmt.Errorf("REPLICATION_FACTOR must be >= 1, got %d", c.ReplicationFactor)
	}
	if c.WriteAcks < 1 || c.WriteAcks > c.ReplicationFactor {
		return fmt.Errorf("WRITE_ACKS must satisfy 1 <= W <= RF (W=%d, RF=%d)", c.WriteAcks, c.ReplicationFactor)
	}
	if c.UploadParallelism < 1 {
		return fmt.Errorf("UPLOAD_PARALLELISM must be >= 1, got %d", c.UploadParallelism)
	}
	if c.MaxConcurrentUploads < 1 {
		return fmt.Errorf("MAX_CONCURRENT_UPLOADS must be >= 1, got %d", c.MaxConcurrentUploads)
	}
	if c.MaxObjectBytes <= 0 {
		return fmt.Errorf("MAX_OBJECT_BYTES must be > 0, got %d", c.MaxObjectBytes)
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("HEARTBEAT_INTERVAL must be > 0")
	}
	if c.SuspectAfter <= 0 {
		return fmt.Errorf("SUSPECT_AFTER must be > 0")
	}
	if c.DeadAfter <= c.SuspectAfter {
		return fmt.Errorf("DEAD_AFTER (%v) must be greater than SUSPECT_AFTER (%v)", c.DeadAfter, c.SuspectAfter)
	}
	if c.RepairDelay < 0 {
		return fmt.Errorf("REPAIR_DELAY must be >= 0")
	}
	if c.StartupGrace < 0 {
		return fmt.Errorf("STARTUP_GRACE must be >= 0")
	}
	if c.ReconcileInterval <= 0 {
		return fmt.Errorf("RECONCILE_INTERVAL must be > 0")
	}
	if c.InventorySyncInterval <= 0 {
		return fmt.Errorf("INVENTORY_SYNC_INTERVAL must be > 0")
	}
	if c.RepairConcurrency < 1 {
		return fmt.Errorf("REPAIR_CONCURRENCY must be >= 1, got %d", c.RepairConcurrency)
	}
	return nil
}
