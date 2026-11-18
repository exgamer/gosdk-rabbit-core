package config

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

func TestRabbitConfig_AmqpURI(t *testing.T) {
	c := RabbitConfig{
		Host:  "127.0.0.1",
		Port:  "5672",
		User:  "guest",
		Pass:  "guest",
		VHost: "/",
	}

	want := "amqp://guest:guest@127.0.0.1:5672/"
	if got := c.AmqpURI(); got != want {
		t.Fatalf("AmqpURI() = %q, want %q", got, want)
	}
}

// TestRabbitConfig_AmqpURI_SpecialChars Пароль и логин со спецсимволами разбираются amqp091 без потерь.
func TestRabbitConfig_AmqpURI_SpecialChars(t *testing.T) {
	for _, pass := range []string{"s3cr%et", "pa#ss", "p/ss", "p@ss", "p:ss?x=1", "%41", "пароль"} {
		c := RabbitConfig{Host: "rabbit.local", Port: "5673", User: "us@er", Pass: pass, VHost: "/main"}

		got, err := amqp091.ParseURI(c.AmqpURI())
		if err != nil {
			t.Fatalf("pass %q: parse: %v", pass, err)
		}

		if got.Password != pass || got.Username != "us@er" {
			t.Errorf("pass %q: got user=%q pass=%q", pass, got.Username, got.Password)
		}

		if got.Host != "rabbit.local" || got.Port != 5673 || got.Vhost != "main" {
			t.Errorf("pass %q: got host=%q port=%d vhost=%q", pass, got.Host, got.Port, got.Vhost)
		}
	}
}

// TestRabbitConfig_AmqpURI_VHost Прежний смысл RABBITMQ_VHOST сохраняется, новые формы не ломают разбор.
func TestRabbitConfig_AmqpURI_VHost(t *testing.T) {
	cases := map[string]string{
		"":        "/",
		"/":       "/",
		"/main":   "main",
		"main":    "main",
		"/a/b":    "a/b",
		"/%2F":    "/",
		"/my vh":  "my vh",
		"/v#1":    "v#1",
		"/100%":   "100%",
		"/orders": "orders",
	}

	for vhost, want := range cases {
		c := RabbitConfig{Host: "h", Port: "5672", User: "u", Pass: "p", VHost: vhost}

		got, err := amqp091.ParseURI(c.AmqpURI())
		if err != nil {
			t.Fatalf("vhost %q: parse %q: %v", vhost, c.AmqpURI(), err)
		}

		if got.Vhost != want {
			t.Errorf("vhost %q: got %q, want %q (uri %q)", vhost, got.Vhost, want, c.AmqpURI())
		}
	}
}

func TestRabbitConfig_AmqpURI_IPv6(t *testing.T) {
	c := RabbitConfig{Host: "::1", Port: "5672", User: "u", Pass: "p", VHost: "/"}

	got, err := amqp091.ParseURI(c.AmqpURI())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if got.Host != "::1" {
		t.Errorf("host = %q, want ::1", got.Host)
	}
}

// TestRabbitConfig_PasswordMasked Пароль не попадает в fmt и JSON, остальные поля - на месте.
func TestRabbitConfig_PasswordMasked(t *testing.T) {
	c := RabbitConfig{Host: "h", Port: "5672", User: "app", Pass: "top-secret", VHost: "/main"}

	outputs := map[string]string{
		"%v":  fmt.Sprintf("%v", c),
		"%+v": fmt.Sprintf("%+v", c),
		"%s":  fmt.Sprintf("%s", c),
		"%#v": fmt.Sprintf("%#v", c),
		"ptr": fmt.Sprintf("%v", &c),
	}

	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	outputs["json"] = string(raw)

	for name, out := range outputs {
		if strings.Contains(out, "top-secret") {
			t.Errorf("%s leaks password: %s", name, out)
		}

		if !strings.Contains(out, "***") || !strings.Contains(out, "app") {
			t.Errorf("%s: want mask and user, got %s", name, out)
		}
	}

	var back map[string]string
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if back["RABBITMQ_PASSWORD"] != "***" || back["RABBITMQ_HOST"] != "h" {
		t.Errorf("json keys: %v", back)
	}

	if c.Pass != "top-secret" {
		t.Errorf("Pass field changed: %q", c.Pass)
	}
}
