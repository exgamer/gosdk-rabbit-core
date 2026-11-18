package rabbitmq

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/exgamer/gosdk-rabbit-core/pkg/config"
	"github.com/exgamer/gosdk-rabbit-core/pkg/marshaler"
	stdamqp "github.com/rabbitmq/amqp091-go"
)

func TestNormalizeDelay(t *testing.T) {
	ok := map[time.Duration]time.Duration{
		time.Second:                         time.Second,
		1500 * time.Millisecond:             2 * time.Second,
		30 * time.Second:                    30 * time.Second,
		24 * time.Hour:                      24 * time.Hour,
		24*time.Hour - 500*time.Millisecond: 24 * time.Hour,
	}

	for in, want := range ok {
		got, err := normalizeDelay(in)
		if err != nil || got != want {
			t.Errorf("normalizeDelay(%s) = %s, %v; want %s", in, got, err, want)
		}
	}

	for _, bad := range []time.Duration{0, -time.Second, 999 * time.Millisecond, 24*time.Hour + time.Millisecond} {
		if _, err := normalizeDelay(bad); err == nil {
			t.Errorf("normalizeDelay(%s): want error", bad)
		}
	}
}

func TestDelayNames(t *testing.T) {
	cases := map[time.Duration]string{
		time.Second:        "1s",
		30 * time.Second:   "30s",
		90 * time.Second:   "90s",
		2 * time.Minute:    "2m",
		time.Hour:          "1h",
		3700 * time.Second: "3700s",
		24 * time.Hour:     "24h",
	}

	for d, suffix := range cases {
		if got := delayQueueName("orders", d); got != "orders.delay."+suffix {
			t.Errorf("delayQueueName(%s) = %q", d, got)
		}

		if got := retryQueueName("orders.created", d); got != "orders.created.retry."+suffix {
			t.Errorf("retryQueueName(%s) = %q", d, got)
		}
	}

	if got := delayQueueName("", 30*time.Second); got != "sdk.default.delay.30s" {
		t.Errorf("default exchange delay name = %q", got)
	}

	if got := parkingQueueName("orders.created"); got != "orders.created.dlq" {
		t.Errorf("parkingQueueName = %q", got)
	}
}

func TestDelayQueueArgs(t *testing.T) {
	args := delayQueueArgs(30*time.Second, "orders", nil)
	if args["x-message-ttl"] != int64(30000) || args["x-dead-letter-exchange"] != "orders" {
		t.Errorf("args = %v", args)
	}

	if _, ok := args["x-dead-letter-routing-key"]; ok {
		t.Error("delay queue must keep original routing keys (no x-dead-letter-routing-key)")
	}

	q := "orders.created"
	retry := delayQueueArgs(5*time.Second, "", &q)
	if retry["x-dead-letter-exchange"] != "" || retry["x-dead-letter-routing-key"] != q {
		t.Errorf("retry args = %v", retry)
	}
}

// TestDecideFailure Ступени повторов, затем .dlq; у очереди со своим DLX - сразу Nack (политика сервиса).
func TestDecideFailure(t *testing.T) {
	delays := []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute}

	for attempt, want := range delays {
		got := decideFailure(attempt, "q", false, delays)
		if got.kind != failureRetry || got.delay != want {
			t.Errorf("attempt %d: got %+v, want retry %s", attempt, got, want)
		}
	}

	if got := decideFailure(3, "q", false, delays); got.kind != failurePark {
		t.Errorf("after limit without DLX: got %+v, want park", got)
	}

	if got := decideFailure(7, "q", false, delays); got.kind != failurePark {
		t.Errorf("beyond limit: got %+v, want park", got)
	}

	for _, attempt := range []int{0, 1, 3} {
		if got := decideFailure(attempt, "q", true, delays); got.kind != failureServiceDLX {
			t.Errorf("own DLX, attempt %d: got %+v, want nack to service DLX policy without SDK retries", attempt, got)
		}
	}

	if got := decideFailure(0, "", true, delays); got.kind != failureServiceDLX {
		t.Errorf("own DLX, server-named queue: got %+v, want nack to service DLX policy", got)
	}

	if got := decideFailure(0, "", false, delays); got.kind != failureNack {
		t.Errorf("no queue name: got %+v, want nack", got)
	}
}

func TestDefaultRetryDelays(t *testing.T) {
	want := []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute, 30 * time.Minute}
	if len(retryDelays) != len(want) {
		t.Fatalf("retryDelays = %v", retryDelays)
	}

	for i := range want {
		if retryDelays[i] != want[i] {
			t.Fatalf("retryDelays = %v, want %v", retryDelays, want)
		}
	}
}

func TestAttemptOf(t *testing.T) {
	cases := map[string]int{"": 0, "0": 0, "3": 3, "x": 0, "-2": 0}

	for header, want := range cases {
		msg := message.NewMessage(watermill.NewUUID(), nil)
		if header != "" {
			msg.Metadata.Set(AttemptHeader, header)
		}

		if got := attemptOf(msg); got != want {
			t.Errorf("attemptOf(%q) = %d, want %d", header, got, want)
		}
	}
}

// TestRepublishing Копия для повтора: тело, метаданные, счётчик, persistent, без служебного x-death-count.
func TestRepublishing(t *testing.T) {
	msg := message.NewMessage("uuid-1", []byte(`{"id":1}`))
	msg.Metadata.Set("trace", "abc")
	msg.Metadata.Set("x-death-count", "2")

	m := marshaler.DeathTolerantMarshaler{DefaultMarshaler: amqp.DefaultMarshaler{NotPersistentDeliveryMode: true}}

	pub, err := republishing(m, msg, map[string]string{AttemptHeader: "1"})
	if err != nil {
		t.Fatalf("republishing: %v", err)
	}

	if string(pub.Body) != `{"id":1}` || pub.DeliveryMode != stdamqp.Persistent {
		t.Errorf("body/mode: %q %d", pub.Body, pub.DeliveryMode)
	}

	if pub.Headers["trace"] != "abc" || pub.Headers[AttemptHeader] != "1" || pub.Headers[amqp.DefaultMessageUUIDHeaderKey] != "uuid-1" {
		t.Errorf("headers: %v", pub.Headers)
	}

	if _, ok := pub.Headers["x-death-count"]; ok {
		t.Errorf("x-death-count must not be carried over: %v", pub.Headers)
	}

	if msg.Metadata.Get(AttemptHeader) != "" {
		t.Error("original message metadata changed")
	}
}

func TestDeathTolerantWrapping(t *testing.T) {
	dm := amqp.DefaultMarshaler{NotPersistentDeliveryMode: true}

	if got, ok := deathTolerant(dm).(marshaler.DeathTolerantMarshaler); !ok || !got.NotPersistentDeliveryMode {
		t.Errorf("value DefaultMarshaler: got %T", deathTolerant(dm))
	}

	if _, ok := deathTolerant(&dm).(marshaler.DeathTolerantMarshaler); !ok {
		t.Errorf("pointer DefaultMarshaler: got %T", deathTolerant(&dm))
	}

	custom := amqp.CorrelatingMarshaler{}
	if _, ok := deathTolerant(custom).(amqp.CorrelatingMarshaler); !ok {
		t.Errorf("custom marshaler must stay: got %T", deathTolerant(custom))
	}

	if deathTolerant(nil) != nil {
		t.Error("nil marshaler must stay nil")
	}
}

func TestHasDeadLetterExchange(t *testing.T) {
	if hasDeadLetterExchange(nil) {
		t.Error("nil args")
	}

	if !hasDeadLetterExchange(stdamqp.Table{"x-dead-letter-exchange": "svc.dlx"}) {
		t.Error("own DLX not detected")
	}

	if !hasDeadLetterExchange(stdamqp.Table{"x-dead-letter-exchange": ""}) {
		t.Error("DLX to default exchange not detected")
	}
}

// serviceMarshaler Свой marshaler сервиса поверх DefaultMarshaler (как dlxMarshaler в сервисах).
type serviceMarshaler struct {
	amqp.DefaultMarshaler
}

// TestServiceMarshalerNotWrapped Свой marshaler сервиса, даже встраивающий DefaultMarshaler, SDK не заменяет.
func TestServiceMarshalerNotWrapped(t *testing.T) {
	if _, ok := deathTolerant(serviceMarshaler{}).(serviceMarshaler); !ok {
		t.Errorf("service marshaler replaced: %T", deathTolerant(serviceMarshaler{}))
	}

	if _, ok := deathTolerant(&serviceMarshaler{}).(*serviceMarshaler); !ok {
		t.Errorf("service marshaler pointer replaced: %T", deathTolerant(&serviceMarshaler{}))
	}
}

// TestOwnDLX_NackWithoutSDKRetry Очередь со своим DLX: ошибка обработчика - Nack исходного, без повторов
// и копий SDK (публикатор повторов не создаётся).
func TestOwnDLX_NackWithoutSDKRetry(t *testing.T) {
	withFakeReporter(t)

	for _, failure := range []string{"error", "panic"} {
		c, err := NewAmqpConsumer(&amqp.ConnectionWrapper{})
		if err != nil {
			t.Fatal(err)
		}

		msg := message.NewMessage(watermill.NewUUID(), []byte(`x`))

		e := &consumerEntry{
			ConsumeConfig: config.ConsumeConfig{Handler: func(context.Context, *message.Message) error {
				if failure == "panic" {
					panic("boom")
				}

				return errors.New("boom")
			}},
			queue:  "q",
			ownDLX: true,
		}

		c.handleMessage(context.Background(), e, msg)

		select {
		case <-msg.Nacked():
		case <-msg.Acked():
			t.Fatalf("%s: message acked", failure)
		case <-time.After(time.Second):
			t.Fatalf("%s: expected Nack", failure)
		}

		if c.retry != nil {
			t.Errorf("%s: SDK retry publisher created for a queue with its own DLX", failure)
		}
	}
}

// TestRetryPublisherCloseRace Публикатор повторов создаётся обработчиком, пока Close его читает (-race);
// после Close новый не создаётся.
func TestRetryPublisherCloseRace(t *testing.T) {
	for i := 0; i < 50; i++ {
		c, err := NewAmqpConsumer(&amqp.ConnectionWrapper{})
		if err != nil {
			t.Fatal(err)
		}

		start := make(chan struct{})
		done := make(chan struct{})

		go func() {
			defer close(done)
			<-start
			_ = c.retryPublisher()
		}()

		close(start)

		if err := c.Close(context.Background()); err != nil {
			t.Fatal(err)
		}

		<-done

		if p := c.retryPublisher(); p != nil {
			c.mu.Lock()
			created := c.retry
			c.mu.Unlock()

			if created != p || !p.closed {
				t.Fatal("publisher created after Close")
			}
		}
	}
}

// TestRetryWithoutConnection_Nacks Без соединения повтор невозможен - сообщение не теряется (Nack).
func TestRetryWithoutConnection_Nacks(t *testing.T) {
	withFakeReporter(t)

	c := &Consumer{onError: ActionRetry, onPanic: ActionRetry}
	msg := message.NewMessage(watermill.NewUUID(), []byte(`x`))

	e := &consumerEntry{
		ConsumeConfig: config.ConsumeConfig{Handler: func(context.Context, *message.Message) error {
			return errors.New("boom")
		}},
		queue: "q",
	}

	c.handleMessage(context.Background(), e, msg)

	select {
	case <-msg.Nacked():
	case <-time.After(time.Second):
		t.Fatal("expected Nack")
	}
}

// TestExplicitAckRespected Явно заданное ActionAck сервиса сохраняется.
func TestExplicitAckRespected(t *testing.T) {
	withFakeReporter(t)

	c := (&Consumer{onError: ActionRetry, onPanic: ActionRetry}).WithErrorAction(ActionAck)
	msg := message.NewMessage(watermill.NewUUID(), []byte(`x`))

	e := &consumerEntry{
		ConsumeConfig: config.ConsumeConfig{Handler: func(context.Context, *message.Message) error {
			return errors.New("boom")
		}},
		queue: "q",
	}

	c.handleMessage(context.Background(), e, msg)

	select {
	case <-msg.Acked():
	case <-time.After(time.Second):
		t.Fatal("expected Ack")
	}
}

// TestCaptureSentry_NoPayloadByDefault Payload и произвольные заголовки не уходят в error-трекер.
func TestCaptureSentry_NoPayloadByDefault(t *testing.T) {
	f := withFakeReporter(t)

	c := &Consumer{onError: ActionAck, onPanic: ActionAck}
	msg := message.NewMessage("uuid-7", []byte(`{"card":"4111111111111111"}`))
	msg.Metadata.Set("authorization", "Bearer secret")
	msg.Metadata.Set(AttemptHeader, "2")

	e := &consumerEntry{
		ConsumeConfig: config.ConsumeConfig{Handler: func(context.Context, *message.Message) error {
			return errors.New("boom")
		}},
		queue:      "payments",
		exchange:   "billing",
		routingKey: "payment.created",
	}

	c.handleMessage(context.Background(), e, msg)

	ctx, ok := f.last().opts.Extra["consumer"].(map[string]any)
	if !ok {
		t.Fatal("no consumer extra")
	}

	for _, key := range []string{"payload", "metadata"} {
		if _, ok := ctx[key]; ok {
			t.Errorf("%s must not be sent by default: %v", key, ctx)
		}
	}

	want := map[string]any{
		"message_uuid": "uuid-7",
		"payload_size": len(msg.Payload),
		"queue":        "payments",
		"exchange":     "billing",
		"routing_key":  "payment.created",
		"attempt":      2,
	}

	for k, v := range want {
		if ctx[k] != v {
			t.Errorf("%s = %v, want %v", k, ctx[k], v)
		}
	}
}

// TestCaptureSentry_PayloadOptIn WithSentryPayloadLimit(n > 0) - payload обрезан до n байт.
func TestCaptureSentry_PayloadOptIn(t *testing.T) {
	f := withFakeReporter(t)

	c := (&Consumer{onError: ActionAck, onPanic: ActionAck}).WithSentryPayloadLimit(4)
	msg := message.NewMessage("uuid-8", []byte(`abcdefgh`))

	e := &consumerEntry{ConsumeConfig: config.ConsumeConfig{Handler: func(context.Context, *message.Message) error {
		return errors.New("boom")
	}}}

	c.handleMessage(context.Background(), e, msg)

	ctx := f.last().opts.Extra["consumer"].(map[string]any)
	if ctx["payload"] != "abcd" || ctx["payload_truncated"] != true {
		t.Errorf("payload opt-in: %v", ctx)
	}
}

func TestRedactURIPassword(t *testing.T) {
	cases := []struct{ uri, text string }{
		{"amqp://user:s3cr%25et@host:abc/vh", `parse "amqp://user:s3cr%25et@host:abc/vh": invalid port`},
		{"amqp://user:p%2Fss@host:5672/", "dial amqp://user:p%2Fss@host:5672/ failed for p/ss"},
		{"amqp://user:plain@host:5672/", "cannot connect amqp://user:plain@host:5672/"},
	}

	for _, c := range cases {
		got := redactURIPassword(c.text, c.uri)

		for _, leak := range []string{"s3cr", "p%2Fss", "p/ss", "plain"} {
			if strings.Contains(got, leak) {
				t.Errorf("%q leaks %q", got, leak)
			}
		}

		if !strings.Contains(got, "***") {
			t.Errorf("no mask in %q", got)
		}
	}

	if got := redactURIPassword("no password", "amqp://host:5672/"); got != "no password" {
		t.Errorf("without password: %q", got)
	}
}

// TestNewAmqpConnection_ErrorWithoutPassword Ошибка разбора URI не содержит пароль.
func TestNewAmqpConnection_ErrorWithoutPassword(t *testing.T) {
	cfg := config.RabbitConfig{Host: "host", Port: "bad-port", User: "u", Pass: "Sup3r#Secret", VHost: "/"}

	_, err := NewAmqpConnection(amqp.ConnectionConfig{AmqpURI: cfg.AmqpURI()})
	if err == nil {
		t.Fatal("expected error")
	}

	if strings.Contains(err.Error(), "Sup3r") {
		t.Fatalf("error leaks password: %v", err)
	}
}

func TestTruncateUTF8(t *testing.T) {
	if got := truncate("привет", 3); got != "п" {
		t.Errorf("truncate = %q", got)
	}

	if got := truncate("abc", 10); got != "abc" {
		t.Errorf("truncate = %q", got)
	}
}
