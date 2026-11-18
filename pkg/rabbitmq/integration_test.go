//go:build integration

package rabbitmq

// Интеграционные тесты с настоящим RabbitMQ. Запуск:
//
//	RABBIT_IT_URI=amqp://guest:guest@127.0.0.1:5672/ go test -tags integration -race -count=1 ./pkg/rabbitmq/
//
// Без RABBIT_IT_URI тесты пропускаются.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	sdkctx "github.com/exgamer/gosdk-core/pkg/context"
	"github.com/exgamer/gosdk-rabbit-core/pkg/config"
	"github.com/exgamer/gosdk-rabbit-core/pkg/marshaler"
	stdamqp "github.com/rabbitmq/amqp091-go"
)

func itURI(t *testing.T) string {
	t.Helper()

	uri := os.Getenv("RABBIT_IT_URI")
	if uri == "" {
		t.Skip("RABBIT_IT_URI not set")
	}

	return uri
}

func itName(prefix string) string {
	return fmt.Sprintf("it.%s.%s", prefix, watermill.NewShortUUID())
}

func itConn(t *testing.T) *amqp.ConnectionWrapper {
	t.Helper()

	conn, err := NewAmqpConnection(amqp.ConnectionConfig{AmqpURI: itURI(t)})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

// rawConn Отдельное соединение amqp091 для проверок: каждая операция - на своём канале
// (ошибка вроде 404 закрывает канал).
type rawConn struct {
	t    *testing.T
	conn *stdamqp.Connection
}

func itRaw(t *testing.T) *rawConn {
	t.Helper()

	conn, err := stdamqp.Dial(itURI(t))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	return &rawConn{t: t, conn: conn}
}

func (r *rawConn) do(fn func(ch *stdamqp.Channel) error) error {
	ch, err := r.conn.Channel()
	if err != nil {
		r.t.Fatalf("channel: %v", err)
	}
	defer func() { _ = ch.Close() }()

	return fn(ch)
}

func (r *rawConn) passive(queue string) (stdamqp.Queue, error) {
	var q stdamqp.Queue

	err := r.do(func(ch *stdamqp.Channel) error {
		var err error
		q, err = ch.QueueDeclarePassive(queue, true, false, false, false, nil)

		return err
	})

	return q, err
}

func declareBoundQueue(t *testing.T, r *rawConn, queue, exchange, kind, key string) {
	t.Helper()

	err := r.do(func(ch *stdamqp.Channel) error {
		if err := ch.ExchangeDeclare(exchange, kind, true, false, false, false, nil); err != nil {
			return err
		}

		if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
			return err
		}

		return ch.QueueBind(queue, key, exchange, false, nil)
	})
	if err != nil {
		t.Fatalf("declare %s: %v", queue, err)
	}
}

func setRetryDelays(t *testing.T, delays ...time.Duration) {
	t.Helper()

	old := retryDelays
	retryDelays = delays

	t.Cleanup(func() { retryDelays = old })
}

func assertEmpty(t *testing.T, r *rawConn, queue string) {
	t.Helper()

	q, err := r.passive(queue)
	if err != nil {
		t.Fatalf("passive %s: %v", queue, err)
	}

	if q.Messages != 0 {
		t.Fatalf("queue %s: %d messages, want 0", queue, q.Messages)
	}
}

// getOne Ждёт сообщение в очереди; очереди может ещё не быть (её объявит SDK).
func getOne(t *testing.T, r *rawConn, queue string, timeout time.Duration) stdamqp.Delivery {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var (
			d  stdamqp.Delivery
			ok bool
		)

		_ = r.do(func(ch *stdamqp.Channel) error {
			var err error
			d, ok, err = ch.Get(queue, true)

			return err
		})

		if ok {
			return d
		}

		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("no message in %s within %s", queue, timeout)

	return stdamqp.Delivery{}
}

// testDelayed Отложенное сообщение приходит не раньше задержки, с исходным routing key, только в свою очередь.
func testDelayed(t *testing.T, kind string, pubCfg func(string) []config.Config, bindKey, otherKey, topic string) {
	raw := itRaw(t)
	exchange := itName(kind)
	queue := exchange + ".q"
	other := exchange + ".other"

	declareBoundQueue(t, raw, queue, exchange, kind, bindKey)
	declareBoundQueue(t, raw, other, exchange, kind, otherKey)

	pub, err := NewAmqpPublisher(itConn(t), pubCfg(exchange)...)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	const delay = 2 * time.Second

	start := time.Now()
	if err := pub.PublishDelayedWithMetaData(topic, map[string]string{"trace": "t-1"}, map[string]int{"id": 7}, delay); err != nil {
		t.Fatalf("publish delayed: %v", err)
	}

	// Пока задержка не истекла - в целевой очереди пусто.
	time.Sleep(delay / 2)
	assertEmpty(t, raw, queue)

	d := getOne(t, raw, queue, 10*time.Second)
	elapsed := time.Since(start)

	if elapsed < delay {
		t.Fatalf("delivered after %s, want >= %s", elapsed, delay)
	}

	if d.RoutingKey != topic || d.Exchange != exchange {
		t.Fatalf("delivered via exchange=%q key=%q, want %q %q", d.Exchange, d.RoutingKey, exchange, topic)
	}

	if string(d.Body) != `{"id":7}` || d.Headers["trace"] != "t-1" || d.DeliveryMode != stdamqp.Persistent {
		t.Fatalf("body=%s headers=%v mode=%d", d.Body, d.Headers, d.DeliveryMode)
	}

	assertEmpty(t, raw, other)

	// Очередь задержки - durable, TTL и DLX по схеме.
	if _, err := raw.passive(exchange + ".delay.2s"); err != nil {
		t.Fatalf("delay queue: %v", err)
	}
}

func TestIntegration_DelayedDirect(t *testing.T) {
	testDelayed(t, "direct", config.NewPublisherDirectDurableConfig, "orders.paid", "orders.created", "orders.paid")
}

func TestIntegration_DelayedTopic(t *testing.T) {
	testDelayed(t, "topic", config.NewPublisherTopicDurableConfig, "orders.*", "invoices.#", "orders.paid")
}

func TestIntegration_DelayedValidation(t *testing.T) {
	pub, err := NewAmqpPublisher(itConn(t), config.NewPublisherDirectDurableConfig(itName("val"))...)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	if err := pub.PublishDelayed("k", 1, 500*time.Millisecond); err == nil {
		t.Fatal("delay below minimum: want error")
	}

	if err := pub.PublishDelayed("k", 1, 25*time.Hour); err == nil {
		t.Fatal("delay above maximum: want error")
	}
}

type poisonRun struct {
	mu    sync.Mutex
	times []time.Time
	ids   []string // id корреляции ctx обработчика по вызовам
}

func (p *poisonRun) record() {
	p.recordID("")
}

func (p *poisonRun) recordID(id string) {
	p.mu.Lock()
	p.times = append(p.times, time.Now())
	p.ids = append(p.ids, id)
	p.mu.Unlock()
}

func (p *poisonRun) idsSnapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]string(nil), p.ids...)
}

func (p *poisonRun) snapshot() []time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]time.Time(nil), p.times...)
}

func startConsumer(t *testing.T, conn *amqp.ConnectionWrapper, handler config.Handler, cfg []config.Config) (*Consumer, <-chan error) {
	t.Helper()

	c, err := NewAmqpConsumer(conn)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	if err := c.RegisterHandler(context.Background(), handler, cfg...); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Очередь и привязка - до публикации, иначе первое сообщение некуда маршрутизировать.
	if err := c.handlers[0].Consumer.SubscribeInitialize(""); err != nil {
		t.Fatalf("subscribe initialize: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- c.Consume(context.Background()) }()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = c.Close(ctx)
	})

	return c, done
}

// TestIntegration_PoisonRetriesThenParking Ошибка обработчика - повторы с паузами, затем <queue>.dlq.
func TestIntegration_PoisonRetriesThenParking(t *testing.T) {
	setRetryDelays(t, time.Second, time.Second, 2*time.Second)

	raw := itRaw(t)
	conn := itConn(t)
	exchange := itName("poison")
	queue := exchange + ".q"

	run := &poisonRun{}
	handler := func(ctx context.Context, msg *message.Message) error {
		run.recordID(sdkctx.CorrelationID(ctx))

		return errors.New("always fails")
	}

	startConsumer(t, conn, handler, config.NewConsumerDirectDurableConfig("it", "k", exchange, queue, 1))

	pub, err := NewAmqpPublisher(conn, config.NewPublisherDirectDurableConfig(exchange)...)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	if err := pub.PublishWithMetaData("k", map[string]string{"trace": "p-1"}, map[string]string{"bad": "msg"}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	parked := getOne(t, raw, queue+".dlq", 20*time.Second)

	times := run.snapshot()
	if len(times) != 4 {
		t.Fatalf("handler calls = %d, want 4 (1 + 3 retries)", len(times))
	}

	wantGaps := []time.Duration{time.Second, time.Second, 2 * time.Second}
	for i, want := range wantGaps {
		if gap := times[i+1].Sub(times[i]); gap < want-100*time.Millisecond {
			t.Errorf("gap %d = %s, want >= %s", i, gap, want)
		}
	}

	if parked.Headers[AttemptHeader] != "3" || parked.Headers[OriginalQueueHeader] != queue ||
		parked.Headers[LastErrorHeader] != "always fails" || parked.Headers["trace"] != "p-1" {
		t.Errorf("parked headers: %v", parked.Headers)
	}

	if string(parked.Body) != `{"bad":"msg"}` || parked.DeliveryMode != stdamqp.Persistent {
		t.Errorf("parked body=%s mode=%d", parked.Body, parked.DeliveryMode)
	}

	// Сообщение без correlation_id: id, выданный при первой обработке, - во всех повторах и в .dlq.
	ids := run.idsSnapshot()
	for i, id := range ids {
		if id == "" || id != ids[0] {
			t.Errorf("handler call %d: correlation id = %q, want %q in every attempt", i, id, ids[0])
		}
	}

	if len(ids) > 0 && parked.Headers[CorrelationIDMetadataKey] != ids[0] {
		t.Errorf("parked correlation_id = %v, want %q", parked.Headers[CorrelationIDMetadataKey], ids[0])
	}

	time.Sleep(500 * time.Millisecond)
	assertEmpty(t, raw, queue)

	if got := len(run.snapshot()); got != 4 {
		t.Errorf("handler called again after parking: %d", got)
	}
}

// ownDLXConfig Конфиг direct-подписчика очереди queue со своим DLX (и x-dead-letter-routing-key, если задан);
// noRequeue - NoRequeueOnNack в конфиге сервиса.
func ownDLXConfig(exchange, queue, key, dlx string, dlxKey *string, prefetch int, noRequeue bool) []config.Config {
	args := stdamqp.Table{"x-dead-letter-exchange": dlx}
	if dlxKey != nil {
		args["x-dead-letter-routing-key"] = *dlxKey
	}

	return config.NewConsumerByConfigs(
		amqp.QueueBindConfig{GenerateRoutingKey: func(string) string { return key }},
		amqp.ConsumeConfig{Consumer: "it", Qos: amqp.QosConfig{PrefetchCount: prefetch}, NoRequeueOnNack: noRequeue},
		amqp.ExchangeConfig{GenerateName: func(string) string { return exchange }, Type: "direct", Durable: true},
		amqp.QueueConfig{GenerateName: func(string) string { return queue }, Durable: true, Arguments: args},
	)
}

// countingMarshaler Свой marshaler сервиса: как dlxMarshaler в сервисах, отдаёт счётчик x-death в x-death-count.
type countingMarshaler struct {
	marshaler.DeathTolerantMarshaler
}

// TestIntegration_OwnDLXServicePolicy Очередь со своим DLX и NoRequeueOnNack в конфиге сервиса (схема
// parking-session: DLX -> TTL-очередь -> обратно): ошибка обработчика - Nack, брокер уводит сообщение в DLX
// с x-death, счётчик растёт на каждом круге; повторов, копий и очередей SDK нет, свой marshaler не заменён.
func TestIntegration_OwnDLXServicePolicy(t *testing.T) {
	setRetryDelays(t, time.Second)

	raw := itRaw(t)
	conn := itConn(t)
	exchange := itName("svcdlx")
	queue := exchange + ".q"
	retryQueue := queue + ".svc-retry"

	// TTL-очередь сервиса: через 300 мс - обратно в queue через exchange по умолчанию
	if err := raw.do(func(ch *stdamqp.Channel) error {
		_, err := ch.QueueDeclare(retryQueue, true, false, false, false, stdamqp.Table{
			"x-message-ttl":             int64(300),
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": queue,
		})

		return err
	}); err != nil {
		t.Fatalf("declare retry queue: %v", err)
	}

	t.Cleanup(func() {
		_ = raw.do(func(ch *stdamqp.Channel) error {
			_, err := ch.QueueDelete(retryQueue, false, false, false)

			return err
		})
	})

	counts := make(chan string, 10)
	done := make(chan struct{})
	var calls atomic.Int32
	handler := func(ctx context.Context, msg *message.Message) error {
		counts <- msg.Metadata.Get("x-death-count")
		if calls.Add(1) < 3 {
			return errors.New("not yet")
		}

		close(done)

		return nil
	}

	cfg := append(ownDLXConfig(exchange, queue, "k", "", &retryQueue, 1, true), config.WithCustomMarshaler(countingMarshaler{}))
	c, _ := startConsumer(t, conn, handler, cfg)

	if _, ok := c.handlers[0].marshaler.(countingMarshaler); !ok {
		t.Fatalf("service marshaler replaced: %T", c.handlers[0].marshaler)
	}

	pub, err := NewAmqpPublisher(conn, config.NewPublisherDirectDurableConfig(exchange)...)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	if err := pub.Publish("k", "x"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatalf("handler calls = %d, want 3 rounds through the service DLX", calls.Load())
	}

	want := []string{"", "1", "2"}
	for i, w := range want {
		if got := <-counts; got != w {
			t.Errorf("round %d: x-death-count = %q, want %q", i, got, w)
		}
	}

	time.Sleep(500 * time.Millisecond)
	assertEmpty(t, raw, queue)
	assertEmpty(t, raw, retryQueue)

	for _, q := range []string{queue + ".dlq", retryQueueName(queue, time.Second)} {
		if _, err := raw.passive(q); err == nil {
			t.Errorf("%s must not be declared for a queue with its own DLX", q)
		}
	}
}

// TestIntegration_OwnDLXWithoutNoRequeue Очередь со своим DLX без NoRequeueOnNack у сервиса: ошибка - Nack
// с возвратом в очередь (как до v1.2.0), в DLX ничего не уходит, повторов SDK нет.
func TestIntegration_OwnDLXWithoutNoRequeue(t *testing.T) {
	setRetryDelays(t, time.Second)

	raw := itRaw(t)
	conn := itConn(t)
	exchange := itName("dlxrequeue")
	queue := exchange + ".q"
	dlx := exchange + ".svc-dlx"
	dlxQueue := dlx + ".q"

	declareBoundQueue(t, raw, dlxQueue, dlx, "fanout", "")

	var calls atomic.Int32
	done := make(chan struct{})
	handler := func(ctx context.Context, msg *message.Message) error {
		if calls.Add(1) == 1 {
			panic("first attempt panics")
		}

		close(done)

		return nil
	}

	startConsumer(t, conn, handler, ownDLXConfig(exchange, queue, "k", dlx, nil, 1, false))

	pub, err := NewAmqpPublisher(conn, config.NewPublisherDirectDurableConfig(exchange)...)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	if err := pub.Publish("k", "x"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("message not redelivered after Nack")
	}

	time.Sleep(300 * time.Millisecond)
	assertEmpty(t, raw, dlxQueue)
	assertEmpty(t, raw, queue)

	if _, err := raw.passive(retryQueueName(queue, time.Second)); err == nil {
		t.Error("SDK retry queue must not be declared for a queue with its own DLX")
	}
}

// TestIntegration_OwnDLXExplicitNackRequeues Явный ActionNack у очереди со своим DLX - возврат в очередь, не в DLX.
func TestIntegration_OwnDLXExplicitNackRequeues(t *testing.T) {
	raw := itRaw(t)
	conn := itConn(t)
	exchange := itName("dlxnack")
	queue := exchange + ".q"
	dlx := exchange + ".svc-dlx"
	dlxQueue := dlx + ".q"

	declareBoundQueue(t, raw, dlxQueue, dlx, "fanout", "")

	var calls atomic.Int32
	done := make(chan struct{})
	handler := func(ctx context.Context, msg *message.Message) error {
		if calls.Add(1) == 1 {
			return errors.New("first attempt fails")
		}

		close(done)

		return nil
	}

	c, err := NewAmqpConsumer(conn)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	c.WithErrorAction(ActionNack)

	if err := c.RegisterHandler(context.Background(), handler, ownDLXConfig(exchange, queue, "k", dlx, nil, 1, false)...); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := c.handlers[0].Consumer.SubscribeInitialize(""); err != nil {
		t.Fatalf("subscribe initialize: %v", err)
	}

	go func() { _ = c.Consume(context.Background()) }()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = c.Close(ctx)
	})

	pub, err := NewAmqpPublisher(conn, config.NewPublisherDirectDurableConfig(exchange)...)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	if err := pub.Publish("k", "x"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("message not redelivered after explicit Nack")
	}

	time.Sleep(300 * time.Millisecond)
	assertEmpty(t, raw, dlxQueue)
}

// TestIntegration_ShutdownOwnDLXUnderLoad Остановка под нагрузкой (prefetch 5, очередь со своим DLX,
// NoRequeueOnNack сервис не задал):
// ни одного здорового сообщения в DLX сервиса, необработанные остаются в очереди.
func TestIntegration_ShutdownOwnDLXUnderLoad(t *testing.T) {
	const total = 15

	for run := 0; run < 5; run++ {
		raw := itRaw(t)
		conn := itConn(t)
		exchange := itName("shutdown")
		queue := exchange + ".q"
		dlx := exchange + ".svc-dlx"
		dlxQueue := dlx + ".q"

		declareBoundQueue(t, raw, dlxQueue, dlx, "fanout", "")

		started := make(chan struct{}, total*4)
		var handled atomic.Int32
		handler := func(ctx context.Context, msg *message.Message) error {
			started <- struct{}{}
			time.Sleep(200 * time.Millisecond)
			handled.Add(1)

			return nil
		}

		c, _ := startConsumer(t, conn, handler, ownDLXConfig(exchange, queue, "k", dlx, nil, 5, false))

		pub, err := NewAmqpPublisher(conn, config.NewPublisherDirectDurableConfig(exchange)...)
		if err != nil {
			t.Fatalf("publisher: %v", err)
		}

		for i := 0; i < total; i++ {
			if err := pub.Publish("k", i); err != nil {
				t.Fatalf("publish: %v", err)
			}
		}

		_ = pub.Close()

		select {
		case <-started:
		case <-time.After(10 * time.Second):
			t.Fatal("handler not started")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = c.Close(ctx)
		cancel()

		if err != nil {
			t.Fatalf("run %d: close: %v", run, err)
		}

		time.Sleep(time.Second)

		dq, _ := raw.passive(dlxQueue)
		q, _ := raw.passive(queue)

		if dq.Messages != 0 {
			t.Errorf("run %d: healthy messages dead-lettered on shutdown: %d", run, dq.Messages)
		}

		if int(handled.Load())+q.Messages < total {
			t.Errorf("run %d: handled=%d + in queue=%d < %d: messages lost", run, handled.Load(), q.Messages, total)
		}
	}
}

// TestIntegration_CloseWaitsForHandler Close прекращает приём и дожидается текущего обработчика,
// ctx обработчика не отменяется, сообщение подтверждается.
func TestIntegration_CloseWaitsForHandler(t *testing.T) {
	raw := itRaw(t)
	conn := itConn(t)
	exchange := itName("stop")
	queue := exchange + ".q"

	started := make(chan struct{})
	var finished atomic.Bool
	var ctxErr atomic.Value

	handler := func(ctx context.Context, msg *message.Message) error {
		close(started)
		time.Sleep(2 * time.Second)
		ctxErr.Store(fmt.Sprint(ctx.Err()))
		finished.Store(true)

		return nil
	}

	c, done := startConsumer(t, conn, handler, config.NewConsumerDirectDurableConfig("it", "k", exchange, queue, 1))

	pub, err := NewAmqpPublisher(conn, config.NewPublisherDirectDurableConfig(exchange)...)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	if err := pub.PublishCtx(context.Background(), "k", "slow"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("handler not started")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := c.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}

	if !finished.Load() {
		t.Fatal("Close returned before the handler finished")
	}

	if got := ctxErr.Load(); got != "<nil>" {
		t.Fatalf("handler ctx cancelled: %v", got)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Consume after Close: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Consume did not return")
	}

	assertEmpty(t, raw, queue)
}

// TestIntegration_PublishCtxCancelled Отменённый ctx - ошибка без публикации.
func TestIntegration_PublishCtxCancelled(t *testing.T) {
	pub, err := NewAmqpPublisher(itConn(t), config.NewPublisherDirectDurableConfig(itName("ctx"))...)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := pub.PublishCtx(ctx, "k", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("PublishCtx with cancelled ctx: %v", err)
	}

	if err := pub.PublishDelayedCtx(ctx, "k", 1, time.Second); err == nil {
		t.Fatal("PublishDelayedCtx with cancelled ctx: want error")
	}
}

// TestIntegration_CorrelationIDPublishToConsumer Id корреляции ctx публикации (PublishCtx, PublishDelayedCtx) доходит
// до ctx обработчика; явная метадата вызывающего главнее.
func TestIntegration_CorrelationIDPublishToConsumer(t *testing.T) {
	conn := itConn(t)
	exchange := itName("corr")
	queue := exchange + ".q"

	got := make(chan string, 4)
	handler := func(ctx context.Context, msg *message.Message) error {
		got <- msg.Metadata.Get("case") + "=" + sdkctx.CorrelationID(ctx)

		return nil
	}

	startConsumer(t, conn, handler, config.NewConsumerDirectDurableConfig("it", "k", exchange, queue, 1))

	pub, err := NewAmqpPublisher(conn, config.NewPublisherDirectDurableConfig(exchange)...)
	if err != nil {
		t.Fatalf("publisher: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	ctx, cancel := context.WithTimeout(sdkctx.WithCorrelationID(context.Background(), "corr-it"), 10*time.Second)
	defer cancel()

	if err := pub.PublishWithMetaDataCtx(ctx, "k", map[string]string{"case": "ctx"}, 1); err != nil {
		t.Fatalf("publish ctx: %v", err)
	}

	if err := pub.PublishWithMetaDataCtx(ctx, "k", map[string]string{"case": "explicit", CorrelationIDMetadataKey: "own"}, 2); err != nil {
		t.Fatalf("publish explicit: %v", err)
	}

	if err := pub.PublishDelayedWithMetaDataCtx(ctx, "k", map[string]string{"case": "delayed"}, 3, time.Second); err != nil {
		t.Fatalf("publish delayed: %v", err)
	}

	want := map[string]bool{"ctx=corr-it": true, "explicit=own": true, "delayed=corr-it": true}

	for range len(want) {
		select {
		case v := <-got:
			if !want[v] {
				t.Errorf("handler saw %q, want one of %v", v, want)
			}

			delete(want, v)
		case <-time.After(15 * time.Second):
			t.Fatalf("not delivered: %v", want)
		}
	}
}
