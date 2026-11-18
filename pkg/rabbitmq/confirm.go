package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	stdamqp "github.com/rabbitmq/amqp091-go"
)

// errUnroutable - брокер вернул сообщение (basic.return): ни одна очередь его не приняла.
var errUnroutable = errors.New("message is unroutable")

// confirmPublisher Публикация в служебную топологию SDK (очереди задержки, повторов, парковки)
// на соединении ядра: канал в режиме confirm, mandatory, подтверждение ждётся в пределах ctx.
// Объявления топологии - лениво, один раз на имя, каждое на своём временном канале
// (ошибка объявления закрывает канал - рабочий канал публикации она не задевает).
type confirmPublisher struct {
	conn *amqp.ConnectionWrapper

	mu      sync.Mutex
	ch      *stdamqp.Channel
	returns chan stdamqp.Return
	closed  bool

	declMu   sync.Mutex
	declared map[string]struct{}
}

func newConfirmPublisher(conn *amqp.ConnectionWrapper) *confirmPublisher {
	return &confirmPublisher{conn: conn, declared: make(map[string]struct{})}
}

// ensure Выполняет declare один раз на key (кэш - под мьютексом).
func (p *confirmPublisher) ensure(key string, declare func(ch *stdamqp.Channel) error) error {
	p.declMu.Lock()
	defer p.declMu.Unlock()

	if _, ok := p.declared[key]; ok {
		return nil
	}

	if err := p.withTempChannel(declare); err != nil {
		return fmt.Errorf("declare %s: %w", key, err)
	}

	p.declared[key] = struct{}{}

	return nil
}

// forget Сбрасывает кэш объявления key (очередь удалили снаружи - объявить заново).
func (p *confirmPublisher) forget(key string) {
	p.declMu.Lock()
	delete(p.declared, key)
	p.declMu.Unlock()
}

func (p *confirmPublisher) withTempChannel(fn func(ch *stdamqp.Channel) error) error {
	conn := p.conn.Connection()
	if conn == nil || conn.IsClosed() {
		return errors.New("not connected to AMQP")
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	return fn(ch)
}

// publishDeclared Публикует msg; если брокер вернул сообщение как немаршрутизируемое -
// сбрасывает кэш объявления key, объявляет заново и публикует ещё раз.
func (p *confirmPublisher) publishDeclared(
	ctx context.Context,
	key string,
	declare func(ch *stdamqp.Channel) error,
	exchange, routingKey string,
	msg stdamqp.Publishing,
) error {
	for try := 0; try < 2; try++ {
		if err := p.ensure(key, declare); err != nil {
			return err
		}

		err := p.publish(ctx, exchange, routingKey, msg)
		if !errors.Is(err, errUnroutable) {
			return err
		}

		p.forget(key)
	}

	return fmt.Errorf("publish to %q (routing key %q): %w", exchange, routingKey, errUnroutable)
}

// publish Публикует msg и ждёт подтверждения брокера в пределах ctx.
func (p *confirmPublisher) publish(ctx context.Context, exchange, routingKey string, msg stdamqp.Publishing) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	ch, err := p.channel()
	if err != nil {
		return err
	}

	dc, err := ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, true, false, msg)
	if err != nil {
		p.dropChannel()

		return fmt.Errorf("publish: %w", err)
	}

	acked, err := dc.WaitContext(ctx)
	if err != nil {
		// Подтверждение не дождались: канал больше не используем, чтобы поздний basic.return
		// этого сообщения не приписался следующему.
		p.dropChannel()

		return fmt.Errorf("publish not confirmed (the message may still be delivered): %w", err)
	}

	if !acked {
		return errors.New("publish nacked by broker")
	}

	// basic.return приходит раньше basic.ack того же сообщения - к этому моменту он уже в канале.
	select {
	case _, returned := <-p.returns:
		if returned {
			return errUnroutable
		}

		return nil // канал закрылся уже после подтверждения - сообщение принято
	default:
		return nil
	}
}

// channel Рабочий канал публикации; закрытый (обрыв, реконнект) - открывается заново.
func (p *confirmPublisher) channel() (*stdamqp.Channel, error) {
	if p.closed {
		return nil, errors.New("publisher closed")
	}

	if p.ch != nil && !p.ch.IsClosed() {
		return p.ch, nil
	}

	conn := p.conn.Connection()
	if conn == nil || conn.IsClosed() {
		return nil, errors.New("not connected to AMQP")
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}

	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()

		return nil, fmt.Errorf("confirm mode: %w", err)
	}

	p.ch = ch
	p.returns = ch.NotifyReturn(make(chan stdamqp.Return, 1))

	return ch, nil
}

func (p *confirmPublisher) dropChannel() {
	if p.ch != nil {
		_ = p.ch.Close()
	}

	p.ch = nil
	p.returns = nil
}

// close Закрывает рабочий канал; соединение ядра не трогает.
func (p *confirmPublisher) close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true
	p.dropChannel()
}
