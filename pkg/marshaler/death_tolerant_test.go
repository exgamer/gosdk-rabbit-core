package marshaler

import (
	"testing"

	wamqp "github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	stdamqp "github.com/rabbitmq/amqp091-go"
)

func TestDeathTolerantMarshaler_RoundTrip(t *testing.T) {
	m := DeathTolerantMarshaler{}

	msg := message.NewMessage("11111111-1111-1111-1111-111111111111", []byte(`{"foo":"bar"}`))
	msg.Metadata.Set("trace_id", "abc-123")

	published, err := m.Marshal(msg)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	got, err := m.Unmarshal(stdamqp.Delivery{Body: published.Body, Headers: published.Headers})
	if err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if got.UUID != msg.UUID {
		t.Fatalf("UUID = %q, want %q", got.UUID, msg.UUID)
	}

	if got.Metadata.Get("trace_id") != "abc-123" {
		t.Fatalf("Metadata[trace_id] = %q, want %q", got.Metadata.Get("trace_id"), "abc-123")
	}
}

func TestDeathTolerantMarshaler_ToleratesXDeathHeader(t *testing.T) {
	m := DeathTolerantMarshaler{}

	delivery := stdamqp.Delivery{
		Body: []byte(`{"foo":"bar"}`),
		Headers: stdamqp.Table{
			wamqp.DefaultMessageUUIDHeaderKey: "11111111-1111-1111-1111-111111111111",
			"trace_id":                        "abc-123",
			"x-death": []interface{}{
				stdamqp.Table{"count": int64(3), "reason": "expired"},
			},
		},
	}

	got, err := m.Unmarshal(delivery)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v, want nil (DefaultMarshaler would fail here)", err)
	}

	if got.Metadata.Get("trace_id") != "abc-123" {
		t.Fatalf("Metadata[trace_id] = %q, want %q", got.Metadata.Get("trace_id"), "abc-123")
	}

	if got.Metadata.Get(deathCountMetadataKey) != "3" {
		t.Fatalf("Metadata[%s] = %q, want %q", deathCountMetadataKey, got.Metadata.Get(deathCountMetadataKey), "3")
	}
}

func TestDeathTolerantMarshaler_DropsOtherNonStringHeaders(t *testing.T) {
	m := DeathTolerantMarshaler{}

	delivery := stdamqp.Delivery{
		Body: []byte(`{}`),
		Headers: stdamqp.Table{
			wamqp.DefaultMessageUUIDHeaderKey: "11111111-1111-1111-1111-111111111111",
			"x-retry-attempts":                int32(2), // не строка, не x-death — должен быть отброшен без ошибки
		},
	}

	got, err := m.Unmarshal(delivery)
	if err != nil {
		t.Fatalf("Unmarshal() error = %v, want nil", err)
	}

	if _, ok := got.Metadata["x-retry-attempts"]; ok {
		t.Fatalf("Metadata[x-retry-attempts] present, want dropped (non-string, non-x-death)")
	}
}

func TestDeathTolerantMarshaler_DefaultMarshalerWouldFail(t *testing.T) {
	delivery := stdamqp.Delivery{
		Body: []byte(`{}`),
		Headers: stdamqp.Table{
			"x-death": []interface{}{stdamqp.Table{"count": int64(1)}},
		},
	}

	if _, err := (wamqp.DefaultMarshaler{}).Unmarshal(delivery); err == nil {
		t.Fatal("DefaultMarshaler.Unmarshal() succeeded unexpectedly on a message with x-death; test assumption is stale")
	}
}
