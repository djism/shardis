package config

import "testing"

// fakeEnv returns a getenv function backed by a map, so tests never touch
// the real process environment (which would make them flaky and unparallelizable).
func fakeEnv(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadPrecedence(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		env      map[string]string
		wantPort int
		wantHost string
	}{
		{"defaults", nil, nil, 7000, "127.0.0.1"},
		{"env overrides default", nil, map[string]string{"SHARDIS_PORT": "7001"}, 7001, "127.0.0.1"},
		{"flag overrides env", []string{"--port", "7002"}, map[string]string{"SHARDIS_PORT": "7001"}, 7002, "127.0.0.1"},
		{"host from env", nil, map[string]string{"SHARDIS_HOST": "0.0.0.0"}, 7000, "0.0.0.0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Load(tc.args, fakeEnv(tc.env))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.Port != tc.wantPort || cfg.Host != tc.wantHost {
				t.Errorf("got %s:%d, want %s:%d", cfg.Host, cfg.Port, tc.wantHost, tc.wantPort)
			}
		})
	}
}

func TestLoadErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
	}{
		{"bad env int", nil, map[string]string{"SHARDIS_PORT": "abc"}},
		{"port too high for bus port", []string{"--port", "60000"}, nil},
		{"port zero", []string{"--port", "0"}, nil},
		{"hz zero", []string{"--hz", "0"}, nil},
		{"hz too high", nil, map[string]string{"SHARDIS_HZ": "501"}},
		{"unknown flag", []string{"--nope"}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Load(tc.args, fakeEnv(tc.env)); err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}
}

func TestEngineFlag(t *testing.T) {
	cfg, err := Load(nil, fakeEnv(nil))
	if err != nil || cfg.Engine != "sharded" {
		t.Fatalf("default engine = %q, %v", cfg.Engine, err)
	}
	cfg, _ = Load([]string{"--engine", "executor"}, fakeEnv(nil))
	if cfg.Engine != "executor" {
		t.Fatalf("engine = %q", cfg.Engine)
	}
}

func TestEnvName(t *testing.T) {
	if got := EnvName("node-timeout"); got != "SHARDIS_NODE_TIMEOUT" {
		t.Errorf("EnvName = %q", got)
	}
}
