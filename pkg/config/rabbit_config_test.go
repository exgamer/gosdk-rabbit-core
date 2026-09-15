package config

import "testing"

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
