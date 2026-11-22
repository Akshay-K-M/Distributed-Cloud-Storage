// These tests verify controller configuration validation.
package controller

import (
	"strings"
	"testing"
	"time"
)

func validTestConfig() Config {
	return Config{
		ClusterToken:          "test-token",
		ChunkSize:             1,
		ReplicationFactor:     1,
		WriteAcks:             1,
		UploadParallelism:     1,
		MaxConcurrentUploads:  1,
		MaxObjectBytes:        1,
		HeartbeatInterval:     time.Second,
		SuspectAfter:          2 * time.Second,
		DeadAfter:             3 * time.Second,
		RepairDelay:           time.Second,
		StartupGrace:          time.Second,
		ReconcileInterval:     time.Second,
		InventorySyncInterval: time.Second,
		RepairConcurrency:     1,
	}
}

func TestConfigValidateRejectsInvalidLoopDurations(t *testing.T) {
	tests := []struct {
		name string
		set  func(*Config)
		want string
	}{
		{"heartbeat", func(c *Config) { c.HeartbeatInterval = 0 }, "HEARTBEAT_INTERVAL"},
		{"repair delay", func(c *Config) { c.RepairDelay = -time.Second }, "REPAIR_DELAY"},
		{"startup grace", func(c *Config) { c.StartupGrace = -time.Second }, "STARTUP_GRACE"},
		{"reconcile", func(c *Config) { c.ReconcileInterval = 0 }, "RECONCILE_INTERVAL"},
		{"inventory sync", func(c *Config) { c.InventorySyncInterval = 0 }, "INVENTORY_SYNC_INTERVAL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			tt.set(&cfg)
			if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() error = %v, want %s", err, tt.want)
			}
		})
	}
}
