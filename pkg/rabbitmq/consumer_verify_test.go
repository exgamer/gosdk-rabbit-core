package rabbitmq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/exgamer/gosdk-core/pkg/errorreporter"
	"github.com/exgamer/gosdk-rabbit-core/pkg/config"
)

type capturedEvent struct {
	err  error
	opts errorreporter.Options
}

type fakeReporter struct {
	mu     sync.Mutex
	events []capturedEvent
}

func (f *fakeReporter) Capture(_ context.Context, err error, opts errorreporter.Options) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, capturedEvent{err: err, opts: opts})
}

func (f *fakeReporter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.events)
}

func (f *fakeReporter) last() capturedEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.events[len(f.events)-1]
}

func withFakeReporter(t *testing.T) *fakeReporter {
	t.Helper()
	f := &fakeReporter{}
	errorreporter.SetReporter(f)
	return f
}

// TestHandleMessage_ErrorPath_ReachesReporter — хендлер вернул error: должен
// уйти Nack (политика ActionNack) и событие должно долететь до errorreporter.
func TestHandleMessage_ErrorPath_ReachesReporter(t *testing.T) {
	f := withFakeReporter(t)

	c := &Consumer{onError: ActionNack, onPanic: ActionNack, sentryPayload: 2048}
	msg := message.NewMessage(watermill.NewUUID(), []byte(`{"city_id":42}`))

	cc := config.ConsumeConfig{
		Handler: func(ctx context.Context, msg *message.Message) error {
			return errors.New("handler failed")
		},
	}

	c.handleMessage(context.Background(), cc, msg)

	select {
	case <-msg.Nacked():
	case <-time.After(time.Second):
		t.Fatal("expected message to be Nacked")
	}

	if got := f.count(); got != 1 {
		t.Fatalf("expected exactly 1 captured event, got %d", got)
	}

	ev := f.last()
	if ev.opts.Level != errorreporter.LevelError {
		t.Fatalf("expected LevelError, got %v", ev.opts.Level)
	}
	if ev.opts.Tags["component"] != "rabbit_consumer" {
		t.Fatalf("expected component=rabbit_consumer tag, got %v", ev.opts.Tags)
	}

	consumerCtx, ok := ev.opts.Extra["consumer"].(map[string]any)
	if !ok {
		t.Fatal("expected 'consumer' extra to be present")
	}
	if consumerCtx["panic"] != false {
		t.Fatalf("expected panic=false for handler error path, got %v", consumerCtx["panic"])
	}
	if consumerCtx["message_uuid"] != msg.UUID {
		t.Fatalf("expected message_uuid=%s, got %v", msg.UUID, consumerCtx["message_uuid"])
	}
}

// TestHandleMessage_PanicPath_ReachesReporter — паника в хендлере: должна
// восстановиться (не уронить процесс), применить onPanic-политику и
// отрепортить с panic=true.
func TestHandleMessage_PanicPath_ReachesReporter(t *testing.T) {
	f := withFakeReporter(t)

	c := &Consumer{onError: ActionNack, onPanic: ActionNack, sentryPayload: 2048}
	msg := message.NewMessage(watermill.NewUUID(), []byte(`panic-case`))

	cc := config.ConsumeConfig{
		Handler: func(ctx context.Context, msg *message.Message) error {
			panic("unexpected nil map write")
		},
	}

	// не должно паниковать наружу - defer/recover внутри handleMessage
	c.handleMessage(context.Background(), cc, msg)

	select {
	case <-msg.Nacked():
	case <-time.After(time.Second):
		t.Fatal("expected message to be Nacked after panic")
	}

	if got := f.count(); got != 1 {
		t.Fatalf("expected exactly 1 captured event, got %d", got)
	}

	consumerCtx, ok := f.last().opts.Extra["consumer"].(map[string]any)
	if !ok {
		t.Fatal("expected 'consumer' extra to be present")
	}
	if consumerCtx["panic"] != true {
		t.Fatalf("expected panic=true for panic path, got %v", consumerCtx["panic"])
	}
}

// TestHandleMessage_Success_DoesNotReachReporter — успешный хендлер не должен
// ничего репортить и должен заакать сообщение.
func TestHandleMessage_Success_DoesNotReachReporter(t *testing.T) {
	f := withFakeReporter(t)

	c := &Consumer{onError: ActionNack, onPanic: ActionNack, sentryPayload: 2048}
	msg := message.NewMessage(watermill.NewUUID(), []byte(`ok`))

	cc := config.ConsumeConfig{
		Handler: func(ctx context.Context, msg *message.Message) error {
			return nil
		},
	}

	c.handleMessage(context.Background(), cc, msg)

	select {
	case <-msg.Acked():
	case <-time.After(time.Second):
		t.Fatal("expected message to be Acked")
	}

	if got := f.count(); got != 0 {
		t.Fatalf("expected 0 captured events on success, got %d", got)
	}
}
