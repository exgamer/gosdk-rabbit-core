package config

import (
	"os"
	"path/filepath"
	"testing"

	coreconfig "github.com/exgamer/gosdk-core/pkg/config"
)

var rabbitEnvKeys = []string{
	"RABBITMQ_HOST",
	"RABBITMQ_PORT",
	"RABBITMQ_USER",
	"RABBITMQ_PASSWORD",
	"RABBITMQ_VHOST",
}

func TestInitRabbitConfig_OSEnvWithoutEnvFile(t *testing.T) {
	clearRabbitEnv(t)
	loadEnvFile(t, "")
	t.Setenv("RABBITMQ_HOST", "rabbit.local")
	t.Setenv("RABBITMQ_PORT", "5673")
	t.Setenv("RABBITMQ_USER", "app")
	t.Setenv("RABBITMQ_PASSWORD", "secret")
	t.Setenv("RABBITMQ_VHOST", "/main")

	cfg, err := InitRabbitConfig()
	if err != nil {
		t.Fatalf("init config: %v", err)
	}

	want := RabbitConfig{
		Host:  "rabbit.local",
		Port:  "5673",
		User:  "app",
		Pass:  "secret",
		VHost: "/main",
	}
	if *cfg != want {
		t.Errorf("got %+v, want %+v", *cfg, want)
	}
}

func TestInitRabbitConfig_OSEnvOverridesEnvFile(t *testing.T) {
	clearRabbitEnv(t)
	loadEnvFile(t, "RABBITMQ_HOST=file\nRABBITMQ_USER=file_user\n")
	t.Setenv("RABBITMQ_HOST", "env")
	t.Setenv("RABBITMQ_PASSWORD", "env_pass")

	cfg, err := InitRabbitConfig()
	if err != nil {
		t.Fatalf("init config: %v", err)
	}

	if cfg.Host != "env" {
		t.Errorf("Host = %q, want env", cfg.Host)
	}
	if cfg.Pass != "env_pass" {
		t.Errorf("Pass = %q, want env_pass from env", cfg.Pass)
	}
	if cfg.User != "file_user" {
		t.Errorf("User = %q, want file_user from .env", cfg.User)
	}
}

func TestInitRabbitConfig_EmptyOSEnvOverridesEnvFile(t *testing.T) {
	clearRabbitEnv(t)
	loadEnvFile(t, "RABBITMQ_PASSWORD=file_pass\nRABBITMQ_VHOST=/file\n")
	t.Setenv("RABBITMQ_PASSWORD", "")
	t.Setenv("RABBITMQ_VHOST", "")

	cfg, err := InitRabbitConfig()
	if err != nil {
		t.Fatalf("init config: %v", err)
	}

	if cfg.Pass != "" || cfg.VHost != "" {
		t.Errorf("got Pass=%q VHost=%q, want empty", cfg.Pass, cfg.VHost)
	}
}

func TestInitRabbitConfig_EnvFileOnly(t *testing.T) {
	clearRabbitEnv(t)
	loadEnvFile(t, "RABBITMQ_HOST=file\n"+
		"RABBITMQ_PORT=5672\n"+
		"RABBITMQ_USER=file_user\n"+
		"RABBITMQ_PASSWORD=file_pass\n"+
		"RABBITMQ_VHOST=/file\n")

	cfg, err := InitRabbitConfig()
	if err != nil {
		t.Fatalf("init config: %v", err)
	}

	want := RabbitConfig{
		Host:  "file",
		Port:  "5672",
		User:  "file_user",
		Pass:  "file_pass",
		VHost: "/file",
	}
	if *cfg != want {
		t.Errorf("got %+v, want %+v", *cfg, want)
	}
}

// clearRabbitEnv Убирает RABBITMQ_* из окружения процесса на время теста.
func clearRabbitEnv(t *testing.T) {
	t.Helper()

	for _, key := range rabbitEnvKeys {
		t.Setenv(key, "")

		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
	}
}

// loadEnvFile Загружает .env с содержимым content; пустой content — файла нет, только окружение.
func loadEnvFile(t *testing.T, content string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), ".env")
	if content != "" {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write env file: %v", err)
		}
	}

	if _, err := coreconfig.LoadEnv(path); err != nil {
		t.Fatalf("load env: %v", err)
	}
}
