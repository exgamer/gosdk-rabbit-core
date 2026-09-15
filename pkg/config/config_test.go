package config

import (
	"testing"

	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	stdamqp "github.com/rabbitmq/amqp091-go"
)

type fakeMarshaler struct{}

func (fakeMarshaler) Marshal(_ *message.Message) (stdamqp.Publishing, error) {
	return stdamqp.Publishing{}, nil
}

func (fakeMarshaler) Unmarshal(_ stdamqp.Delivery) (*message.Message, error) {
	return nil, nil
}

func TestWithCustomMarshaler(t *testing.T) {
	var cfg amqp.Config

	WithCustomMarshaler(fakeMarshaler{})(&cfg)

	if _, ok := cfg.Marshaler.(fakeMarshaler); !ok {
		t.Fatalf("WithCustomMarshaler() did not set config.Marshaler to the provided implementation, got %T", cfg.Marshaler)
	}
}

func TestWithConnectionConfig(t *testing.T) {
	var cfg amqp.Config

	want := amqp.ConnectionConfig{AmqpURI: "amqp://example"}
	WithConnectionConfig(want)(&cfg)

	if cfg.Connection != want {
		t.Fatalf("WithConnectionConfig() = %+v, want %+v", cfg.Connection, want)
	}
}
