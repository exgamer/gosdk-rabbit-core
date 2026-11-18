package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/exgamer/gosdk-rabbit-core/pkg/config"
	"github.com/google/uuid"
	stdamqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	pub    *amqp.Publisher
	logger watermill.LoggerAdapter

	conn *amqp.ConnectionWrapper
	cfg  amqp.Config

	delayedOnce sync.Once
	delayed     *confirmPublisher

	mu     sync.Mutex
	closed bool
}

func NewAmqpPublisher(conn *amqp.ConnectionWrapper, cfg ...config.Config) (*Publisher, error) {
	if conn == nil {
		return nil, errors.New("nil rabbit connection")
	}

	cc := &amqp.Config{}
	for _, opt := range cfg {
		opt(cc)
	}

	logger := newWatermillLogger()

	pub, err := amqp.NewPublisherWithConnection(*cc, logger, conn)
	if err != nil {
		return nil, fmt.Errorf("error creating publisher: %w", err)
	}

	return &Publisher{
		pub:    pub,
		logger: logger,
		conn:   conn,
		cfg:    *cc,
	}, nil
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}
	p.closed = true

	if p.delayed != nil {
		p.delayed.close()
	}

	return p.pub.Close()
}

// Publish - отправка одного сообщения (topic = routing key для direct/topic, см. config ниже)
func (p *Publisher) Publish(topic string, payload any) error {
	msg, err := messageForm(payload)
	if err != nil {
		return err
	}

	if err := p.pub.Publish(topic, msg); err != nil {
		return fmt.Errorf("error publishing message: %w", err)
	}

	return nil
}

func (p *Publisher) PublishWithMetaData(topic string, meta map[string]string, payload any) error {
	msg, err := messageFormWithMetaData(meta, payload)
	if err != nil {
		return err
	}

	if err := p.pub.Publish(topic, msg); err != nil {
		return fmt.Errorf("error publishing message: %w", err)
	}

	return nil
}

func (p *Publisher) PublishBatch(topic string, payloads []any) error {
	msgs, err := messagesForm(payloads)
	if err != nil {
		return fmt.Errorf("error creating messages: %w", err)
	}
	if len(msgs) == 0 {
		return nil
	}

	if err := p.pub.Publish(topic, msgs...); err != nil {
		return fmt.Errorf("error publishing messages: %w", err)
	}

	return nil
}

func (p *Publisher) PublishBatchWithMetaData(topic string, meta map[string]string, payloads []any) error {
	msgs, err := messagesFormWithMetaData(meta, payloads)
	if err != nil {
		return fmt.Errorf("error creating messages: %w", err)
	}
	if len(msgs) == 0 {
		return nil
	}

	if err := p.pub.Publish(topic, msgs...); err != nil {
		return fmt.Errorf("error publishing messages: %w", err)
	}

	return nil
}

// --- публикация с ctx

// PublishCtx - Publish, ограниченный ctx: отправка и ожидание подтверждения брокера (ConfirmDelivery).
// Id корреляции ctx (sdkctx.CorrelationID) уходит в метаданные correlation_id, если вызывающий не задал свой;
// так во всех методах *Ctx и *Delayed*. Методы без ctx id не ставят (только явная метадата).
// Если ctx истёк раньше подтверждения - ошибка ctx; сообщение при этом могло дойти.
func (p *Publisher) PublishCtx(ctx context.Context, topic string, payload any) error {
	msg, err := messageForm(payload)
	if err != nil {
		return err
	}

	return p.publishCtx(ctx, topic, []*message.Message{msg})
}

// PublishWithMetaDataCtx - PublishWithMetaData, ограниченный ctx (см. PublishCtx).
func (p *Publisher) PublishWithMetaDataCtx(ctx context.Context, topic string, meta map[string]string, payload any) error {
	msg, err := messageFormWithMetaData(meta, payload)
	if err != nil {
		return err
	}

	return p.publishCtx(ctx, topic, []*message.Message{msg})
}

// PublishBatchCtx - PublishBatch, ограниченный ctx (см. PublishCtx).
func (p *Publisher) PublishBatchCtx(ctx context.Context, topic string, payloads []any) error {
	msgs, err := messagesForm(payloads)
	if err != nil {
		return fmt.Errorf("error creating messages: %w", err)
	}

	return p.publishCtx(ctx, topic, msgs)
}

// PublishBatchWithMetaDataCtx - PublishBatchWithMetaData, ограниченный ctx (см. PublishCtx).
func (p *Publisher) PublishBatchWithMetaDataCtx(ctx context.Context, topic string, meta map[string]string, payloads []any) error {
	msgs, err := messagesFormWithMetaData(meta, payloads)
	if err != nil {
		return fmt.Errorf("error creating messages: %w", err)
	}

	return p.publishCtx(ctx, topic, msgs)
}

func (p *Publisher) publishCtx(ctx context.Context, topic string, msgs []*message.Message) error {
	if len(msgs) == 0 {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	setCorrelationID(ctx, msgs)

	for _, msg := range msgs {
		msg.SetContext(ctx)
	}

	// watermill ждёт подтверждение без ctx - ожидание ограничивается здесь;
	// горутина завершится сама, когда придёт подтверждение или закроется канал.
	done := make(chan error, 1)
	go func() { done <- p.pub.Publish(topic, msgs...) }()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("error publishing message: %w", err)
		}

		return nil
	case <-ctx.Done():
		return fmt.Errorf("publish not confirmed (the message may still be delivered): %w", ctx.Err())
	}
}

// --- отложенные сообщения

// PublishDelayed - сообщение придёт в exchange публикатора с тем же routing key не раньше чем через delay.
//
// Схема: fanout-exchange и очередь задержки <exchange>.delay.<delay> (например orders.delay.30s),
// durable, x-message-ttl = delay, x-dead-letter-exchange = exchange публикатора. Сообщение
// публикуется в exchange задержки с исходным routing key и по истечении TTL уходит в целевой
// exchange с ним же (для direct, topic и fanout одинаково). Объявляются лениво, один раз.
//
// delay - от MinPublishDelay до MaxPublishDelay, доли секунды округляются вверх. Каждая уникальная
// задержка - своя очередь: используйте фиксированный набор значений.
// Публикация persistent, с подтверждением брокера.
func (p *Publisher) PublishDelayed(topic string, payload any, delay time.Duration) error {
	return p.PublishDelayedCtx(context.Background(), topic, payload, delay)
}

// PublishDelayedWithMetaData - PublishDelayed с метаданными (заголовками) сообщения.
func (p *Publisher) PublishDelayedWithMetaData(topic string, meta map[string]string, payload any, delay time.Duration) error {
	return p.PublishDelayedWithMetaDataCtx(context.Background(), topic, meta, payload, delay)
}

// PublishBatchDelayed - PublishDelayed для набора сообщений.
func (p *Publisher) PublishBatchDelayed(topic string, payloads []any, delay time.Duration) error {
	return p.PublishBatchDelayedCtx(context.Background(), topic, payloads, delay)
}

// PublishBatchDelayedWithMetaData - PublishDelayedWithMetaData для набора сообщений.
func (p *Publisher) PublishBatchDelayedWithMetaData(topic string, meta map[string]string, payloads []any, delay time.Duration) error {
	return p.PublishBatchDelayedWithMetaDataCtx(context.Background(), topic, meta, payloads, delay)
}

// PublishDelayedCtx - PublishDelayed, ограниченный ctx (объявление очереди и подтверждение брокера).
func (p *Publisher) PublishDelayedCtx(ctx context.Context, topic string, payload any, delay time.Duration) error {
	msg, err := messageForm(payload)
	if err != nil {
		return err
	}

	return p.publishDelayed(ctx, topic, []*message.Message{msg}, delay)
}

// PublishDelayedWithMetaDataCtx - PublishDelayedWithMetaData, ограниченный ctx.
func (p *Publisher) PublishDelayedWithMetaDataCtx(ctx context.Context, topic string, meta map[string]string, payload any, delay time.Duration) error {
	msg, err := messageFormWithMetaData(meta, payload)
	if err != nil {
		return err
	}

	return p.publishDelayed(ctx, topic, []*message.Message{msg}, delay)
}

// PublishBatchDelayedCtx - PublishBatchDelayed, ограниченный ctx.
func (p *Publisher) PublishBatchDelayedCtx(ctx context.Context, topic string, payloads []any, delay time.Duration) error {
	msgs, err := messagesForm(payloads)
	if err != nil {
		return fmt.Errorf("error creating messages: %w", err)
	}

	return p.publishDelayed(ctx, topic, msgs, delay)
}

// PublishBatchDelayedWithMetaDataCtx - PublishBatchDelayedWithMetaData, ограниченный ctx.
func (p *Publisher) PublishBatchDelayedWithMetaDataCtx(ctx context.Context, topic string, meta map[string]string, payloads []any, delay time.Duration) error {
	msgs, err := messagesFormWithMetaData(meta, payloads)
	if err != nil {
		return fmt.Errorf("error creating messages: %w", err)
	}

	return p.publishDelayed(ctx, topic, msgs, delay)
}

func (p *Publisher) publishDelayed(ctx context.Context, topic string, msgs []*message.Message, delay time.Duration) error {
	d, err := normalizeDelay(delay)
	if err != nil {
		return err
	}

	if len(msgs) == 0 {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	setCorrelationID(ctx, msgs)

	pub, err := p.delayedPublisher()
	if err != nil {
		return err
	}

	exchange := generate(p.cfg.Exchange.GenerateName, topic)

	routingKey := topic
	if p.cfg.Publish.GenerateRoutingKey != nil {
		routingKey = p.cfg.Publish.GenerateRoutingKey(topic)
	}

	// Целевой exchange должен существовать к истечению TTL, иначе брокер молча выбросит сообщение.
	if exchange != "" {
		if err := pub.ensure("exchange:"+exchange, func(ch *stdamqp.Channel) error {
			return p.topologyBuilder().ExchangeDeclare(ch, exchange, p.cfg)
		}); err != nil {
			return err
		}
	}

	name := delayQueueName(exchange, d)
	declare := func(ch *stdamqp.Channel) error {
		return declareDelayRoute(ch, name, exchange, d)
	}

	marshaler := p.cfg.Marshaler
	if marshaler == nil {
		marshaler = amqp.DefaultMarshaler{}
	}

	for _, msg := range msgs {
		publishing, err := marshaler.Marshal(msg)
		if err != nil {
			return fmt.Errorf("cannot marshal message: %w", err)
		}

		publishing.DeliveryMode = stdamqp.Persistent

		if err := pub.publishDeclared(ctx, name, declare, name, routingKey, publishing); err != nil {
			return fmt.Errorf("error publishing delayed message: %w", err)
		}
	}

	return nil
}

// declareDelayRoute fanout-exchange name -> очередь name (TTL d, dead-letter в exchange).
func declareDelayRoute(ch *stdamqp.Channel, name, exchange string, d time.Duration) error {
	if err := ch.ExchangeDeclare(name, "fanout", true, false, false, false, nil); err != nil {
		return err
	}

	if _, err := ch.QueueDeclare(name, true, false, false, false, delayQueueArgs(d, exchange, nil)); err != nil {
		return err
	}

	return ch.QueueBind(name, "", name, false, nil)
}

func (p *Publisher) topologyBuilder() amqp.TopologyBuilder {
	if p.cfg.TopologyBuilder != nil {
		return p.cfg.TopologyBuilder
	}

	return &amqp.DefaultTopologyBuilder{}
}

func (p *Publisher) delayedPublisher() (*confirmPublisher, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, errors.New("publisher closed")
	}

	if p.conn == nil {
		return nil, errors.New("nil rabbit connection")
	}

	p.delayedOnce.Do(func() {
		p.delayed = newConfirmPublisher(p.conn)
	})

	return p.delayed, nil
}

// --- message helpers

func messageForm(payload any) (*message.Message, error) {
	val, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}
	return message.NewMessage(uuid.New().String(), val), nil
}

func messagesForm[T any](payloads []T) ([]*message.Message, error) {
	out := make([]*message.Message, 0, len(payloads))
	for _, payload := range payloads {
		msg, err := messageForm(payload)
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	return out, nil
}

func messageFormWithMetaData(meta map[string]string, payload any) (*message.Message, error) {
	val, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	msg := message.NewMessage(uuid.New().String(), val)
	for k, v := range meta {
		msg.Metadata.Set(k, v)
	}
	return msg, nil
}

func messagesFormWithMetaData[T any](meta map[string]string, payloads []T) ([]*message.Message, error) {
	out := make([]*message.Message, 0, len(payloads))
	for _, payload := range payloads {
		msg, err := messageFormWithMetaData(meta, payload)
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	return out, nil
}
