package rabbitmq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	sdkctx "github.com/exgamer/gosdk-core/pkg/context"
	"github.com/exgamer/gosdk-rabbit-core/pkg/config"
	"github.com/exgamer/gosdk-rabbit-core/pkg/marshaler"
)

func TestSetCorrelationID(t *testing.T) {
	ctx := sdkctx.WithCorrelationID(context.Background(), "corr-1")

	plain := message.NewMessage("1", nil)
	own := message.NewMessage("2", nil)
	own.Metadata.Set(CorrelationIDMetadataKey, "explicit")
	noMeta := &message.Message{UUID: "3"}

	setCorrelationID(ctx, []*message.Message{plain, own, noMeta})

	if got := plain.Metadata.Get(CorrelationIDMetadataKey); got != "corr-1" {
		t.Errorf("plain: correlation_id = %q, want corr-1", got)
	}

	if got := own.Metadata.Get(CorrelationIDMetadataKey); got != "explicit" {
		t.Errorf("explicit metadata: correlation_id = %q, want explicit (caller wins)", got)
	}

	if got := noMeta.Metadata.Get(CorrelationIDMetadataKey); got != "corr-1" {
		t.Errorf("nil metadata: correlation_id = %q, want corr-1", got)
	}
}

func TestSetCorrelationID_NoIDNoMetadata(t *testing.T) {
	msg := message.NewMessage("1", nil)

	setCorrelationID(context.Background(), []*message.Message{msg})

	if _, ok := msg.Metadata[CorrelationIDMetadataKey]; ok {
		t.Fatalf("correlation_id set without an id in ctx: %v", msg.Metadata)
	}
}

// handlerSeen runs handleMessage with a handler that records the correlation id of its ctx and of msg.Context().
func handlerSeen(t *testing.T, base context.Context, msg *message.Message, fail bool) (ctxID, msgCtxID string) {
	t.Helper()

	c := &Consumer{onError: ActionAck, onPanic: ActionAck}
	e := &consumerEntry{ConsumeConfig: config.ConsumeConfig{Handler: func(ctx context.Context, m *message.Message) error {
		ctxID = sdkctx.CorrelationID(ctx)
		msgCtxID = sdkctx.CorrelationID(m.Context())

		if fail {
			return errors.New("boom")
		}

		return nil
	}}}

	c.handleMessage(base, e, msg)

	select {
	case <-msg.Acked():
	case <-time.After(time.Second):
		t.Fatal("expected Ack")
	}

	return ctxID, msgCtxID
}

func TestHandleMessage_CorrelationIDFromMetadata(t *testing.T) {
	msg := message.NewMessage(watermill.NewUUID(), []byte(`x`))
	msg.Metadata.Set(CorrelationIDMetadataKey, "from-msg")

	// The consumer ctx id (if any) does not override the message's own id.
	base := sdkctx.WithCorrelationID(context.Background(), "consumer-ctx")

	ctxID, msgCtxID := handlerSeen(t, base, msg, false)

	if ctxID != "from-msg" || msgCtxID != "from-msg" {
		t.Fatalf("handler ctx id = %q, msg.Context() id = %q, want from-msg", ctxID, msgCtxID)
	}
}

// Without correlation_id each message gets its own new id, also written to its metadata (retry/.dlq keep it).
func TestHandleMessage_CorrelationIDGenerated(t *testing.T) {
	base := sdkctx.WithCorrelationID(context.Background(), "consumer-ctx")

	first := message.NewMessage(watermill.NewUUID(), []byte(`x`))
	second := message.NewMessage(watermill.NewUUID(), []byte(`x`))

	id1, _ := handlerSeen(t, base, first, true)
	id2, _ := handlerSeen(t, base, second, false)

	if id1 == "" || id2 == "" || id1 == id2 || id1 == "consumer-ctx" || id2 == "consumer-ctx" {
		t.Fatalf("generated ids = %q, %q, want two distinct new ids", id1, id2)
	}

	if got := first.Metadata.Get(CorrelationIDMetadataKey); got != id1 {
		t.Fatalf("metadata correlation_id = %q, want %q", got, id1)
	}
}

// Retry and parking copies keep the original correlation_id.
func TestRepublishing_KeepsCorrelationID(t *testing.T) {
	msg := message.NewMessage("uuid-1", []byte(`{}`))
	msg.Metadata.Set(CorrelationIDMetadataKey, "orig")

	m := marshaler.DeathTolerantMarshaler{DefaultMarshaler: amqp.DefaultMarshaler{}}

	for _, headers := range []map[string]string{
		{AttemptHeader: "1"},
		{AttemptHeader: "3", OriginalQueueHeader: "q", LastErrorHeader: "x"},
	} {
		pub, err := republishing(m, msg, headers)
		if err != nil {
			t.Fatalf("republishing: %v", err)
		}

		if pub.Headers[CorrelationIDMetadataKey] != "orig" {
			t.Fatalf("headers %v: correlation_id = %v, want orig", headers, pub.Headers[CorrelationIDMetadataKey])
		}
	}
}
