package di

import (
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/exgamer/gosdk-core/pkg/di"
	"github.com/exgamer/gosdk-rabbit-core/pkg/app"
	"github.com/exgamer/gosdk-rabbit-core/pkg/rabbitmq"
)

// GetRabbitClient возвращает клиент rabbit
func GetRabbitClient(c *di.Container) (*rabbitmq.Consumer, error) {
	client, err := di.Resolve[*rabbitmq.Consumer](c)

	if err != nil {
		return nil, err
	}

	return client, nil
}

// GetRabbitConnection возвращает общее соединение kernel-а. Нужно, когда требуется декларация
// топологии (exchange/queue/bind) без собственного consumer-а или publisher-а — см.
// rabbitmq.DeclareTopology. Не открывайте отдельное amqp.Dial-соединение только под это.
func GetRabbitConnection(c *di.Container) (*amqp.ConnectionWrapper, error) {
	conn, err := di.Resolve[*amqp.ConnectionWrapper](c)

	if err != nil {
		return nil, err
	}

	return conn, nil
}

// GetRabbitConsumersRegistry возвращает клиент регситр консьюмеров
func GetRabbitConsumersRegistry(c *di.Container) (*app.ConsumersRegistry, error) {
	client, err := di.Resolve[*app.ConsumersRegistry](c)

	if err != nil {
		return nil, err
	}

	return client, nil
}

// GetRabbitPublishersRegistry возвращает клиент регситр publishers
func GetRabbitPublishersRegistry(c *di.Container) (*app.PublisherRegistry, error) {
	client, err := di.Resolve[*app.PublisherRegistry](c)

	if err != nil {
		return nil, err
	}

	return client, nil
}
