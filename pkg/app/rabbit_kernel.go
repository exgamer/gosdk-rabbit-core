package app

import (
	"context"
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

	enableConsumer  bool
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

		go func() {
			if err := k.consumer.Consume(k.ctx); err != nil {
				logger.Error(k.ctx, "rabbit consumer stopped: "+err.Error())
				k.cancel()
			}
		}()
	}

	return nil
}

func (k *RabbitKernel) Stop(ctx context.Context) error {
	if k.cancel != nil {
		k.cancel()
	}

	if k.publishersRegistry != nil {
		_ = k.publishersRegistry.Close()
	}

	if k.connection == nil {
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- k.connection.Close() }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
