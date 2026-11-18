package app

import (
	"context"
	"errors"

	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/exgamer/gosdk-core/pkg/app"
	"github.com/exgamer/gosdk-core/pkg/di"
	"github.com/exgamer/gosdk-core/pkg/logger"
	"github.com/exgamer/gosdk-rabbit-core/pkg/config"
	"github.com/exgamer/gosdk-rabbit-core/pkg/rabbitmq"
)

const RabbitKernelName = "rabbit"

func NewRabbitKernel() *RabbitKernel {
	return &RabbitKernel{}
}

type RabbitKernel struct {
	connection *amqp.ConnectionWrapper
	consumer   *rabbitmq.Consumer
	config     *config.RabbitConfig

	consumersRegistry  *ConsumersRegistry
	publishersRegistry *PublisherRegistry

	ctx    context.Context
	cancel context.CancelFunc

	// consumeDone закрывается, когда Consume вернулся
	consumeDone chan struct{}

	enableConsumer bool
	// enablePublisher - флаг публичного EnablePublisher; publisher-ы создаются реестром по запросу
	// независимо от него, поле оставлено ради совместимости API.
	enablePublisher bool
}

func (k *RabbitKernel) EnableConsumer() *RabbitKernel {
	k.enableConsumer = true

	return k
}

func (k *RabbitKernel) EnablePublisher() *RabbitKernel {
	k.enablePublisher = true

	return k
}

func (k *RabbitKernel) Name() string {
	return RabbitKernelName
}

func (k *RabbitKernel) Init(a *app.App) error {
	rabbitConfig, err := config.InitRabbitConfig()

	if err != nil {
		return err
	}

	k.config = rabbitConfig

	connection, err := rabbitmq.NewAmqpConnection(amqp.ConnectionConfig{
		AmqpURI: k.config.AmqpURI(),
	})

	if err != nil {
		return err
	}

	k.connection = connection

	// Соединение в DI (чтобы модули могли декларировать топологию — exchange/queue/bind —
	// без необходимости поднимать consumer или publisher, см. rabbitmq.DeclareTopology)
	di.Register(a.Container, k.connection)

	// Реестр в DI (чтобы модули могли добавлять publishers)
	pubReg := NewPublisherRegistry(k.connection)
	di.Register(a.Container, pubReg)
	k.publishersRegistry = pubReg

	// Реестр в DI (чтобы модули могли добавлять handlers)
	reg := NewConsumersRegistry()
	di.Register(a.Container, reg)
	k.consumersRegistry = reg

	return nil
}

func (k *RabbitKernel) Start(a *app.App) error {
	// Контекст на весь runtime
	k.ctx, k.cancel = context.WithCancel(a.GetContext())

	if k.enableConsumer {
		consumer, err := rabbitmq.NewAmqpConsumer(k.connection)

		if err != nil {
			return err
		}

		di.Register(a.Container, consumer)

		k.consumer = consumer

		handlers := k.consumersRegistry.List()

		// Регистрируем консьюмеры
		if err = k.consumer.RegisterMultipleHandler(k.ctx, handlers); err != nil {
			return err
		}

		k.consumeDone = make(chan struct{})

		go k.runConsumer(a)
	}

	return nil
}

// runConsumer Consume до остановки. Если Consume вернулся не из-за Stop / отмены ctx приложения
// (ошибка подписки, закрытый канал сообщений) - приложение останавливается через App.Fail:
// сервис без приёма сообщений не остаётся «живым».
func (k *RabbitKernel) runConsumer(a *app.App) {
	defer close(k.consumeDone)

	err := k.consumer.Consume(k.ctx)
	if k.ctx.Err() != nil {
		if err != nil {
			logger.Error(k.ctx, "rabbit consumer stopped: "+err.Error())
		}

		return
	}

	if err == nil {
		err = errors.New("rabbit consumer stopped unexpectedly")
	}

	logger.Error(k.ctx, "rabbit consumer stopped: "+err.Error())
	k.cancel()
	a.Fail(err)
}

// Stop прекращает приём сообщений, ждёт текущие обработчики в пределах ctx, затем закрывает
// подписчиков, publisher-ы и соединение.
func (k *RabbitKernel) Stop(ctx context.Context) error {
	if k.cancel != nil {
		k.cancel()
	}

	var errs []error

	if k.consumer != nil {
		if err := k.consumer.Close(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	if k.consumeDone != nil {
		select {
		case <-k.consumeDone:
		case <-ctx.Done():
		}
	}

	if k.publishersRegistry != nil {
		_ = k.publishersRegistry.Close()
	}

	if k.connection == nil {
		return errors.Join(errs...)
	}

	done := make(chan error, 1)
	go func() { done <- k.connection.Close() }()

	select {
	case err := <-done:
		errs = append(errs, err)
	case <-ctx.Done():
		errs = append(errs, ctx.Err())
	}

	return errors.Join(errs...)
}
