package config

import (
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/exgamer/gosdk-rabbit-core/pkg/enums"
	"github.com/exgamer/gosdk-rabbit-core/pkg/marshaler"
)

// NewConsumerByConfigs - возвращает новый конфиг.
// Marshaler - marshaler.DeathTolerantMarshaler: сообщения, вернувшиеся через DLX (с заголовком
// x-death, в том числе повторы SDK), разбираются, а не уходят в бесконечный Nack.
func NewConsumerByConfigs(bind amqp.QueueBindConfig, consumer amqp.ConsumeConfig, exchange amqp.ExchangeConfig, queue amqp.QueueConfig) []Config {
	return []Config{
		WithCustomMarshaler(marshaler.DeathTolerantMarshaler{
			DefaultMarshaler: amqp.DefaultMarshaler{NotPersistentDeliveryMode: true},
		}),
		WithRoutingKeyBinding(bind),
		WithConsumerConfig(consumer),
		WithExchangeConfig(exchange),
		WithQueueName(queue),
		WithTopologyBuilder(&amqp.DefaultTopologyBuilder{}),
	}
}

// NewConsumerTopicDurableConfig - дефолтный конфиг для topic консьюмера
func NewConsumerTopicDurableConfig(consumerTag, routingKey, exchange, queue string, prefetchCnt int) []Config {
	return NewConsumerByConfigs(
		amqp.QueueBindConfig{
			GenerateRoutingKey: func(topic string) string { return routingKey },
		},
		amqp.ConsumeConfig{
			Consumer: consumerTag,
			Qos:      amqp.QosConfig{PrefetchCount: prefetchCnt},
		},
		amqp.ExchangeConfig{
			GenerateName: func(topic string) string {
				return exchange
			},
			Type:    enums.TopicExchange,
			Durable: true,
		},
		amqp.QueueConfig{
			GenerateName: func(topic string) string {
				return queue
			},
			Durable: true,
		},
	)
}

// NewConsumerDirectDurableConfig - дефолтный конфиг для direct консьюмера
func NewConsumerDirectDurableConfig(consumerTag, routingKey, exchange, queue string, prefetchCnt int) []Config {
	return NewConsumerByConfigs(
		amqp.QueueBindConfig{
			GenerateRoutingKey: func(topic string) string { return routingKey },
		},
		amqp.ConsumeConfig{
			Consumer: consumerTag,
			Qos:      amqp.QosConfig{PrefetchCount: prefetchCnt},
		},
		amqp.ExchangeConfig{
			GenerateName: func(topic string) string {
				return exchange
			},
			Type:    enums.DirectExchange,
			Durable: true,
		},
		amqp.QueueConfig{
			GenerateName: func(topic string) string {
				return queue
			},
			Durable: true,
		},
	)
}

// NewConsumerFanoutDurableConfig - дефолтный конфиг для fanout консьюмера
func NewConsumerFanoutDurableConfig(consumerTag, routingKey, exchange, queue string, prefetchCnt int) []Config {
	return NewConsumerByConfigs(
		amqp.QueueBindConfig{
			GenerateRoutingKey: func(topic string) string { return routingKey },
		},
		amqp.ConsumeConfig{
			Consumer: consumerTag,
			Qos:      amqp.QosConfig{PrefetchCount: prefetchCnt},
		},
		amqp.ExchangeConfig{
			GenerateName: func(topic string) string {
				return exchange
			},
			Type:    enums.FanoutExchange,
			Durable: true,
		},
		amqp.QueueConfig{
			GenerateName: func(topic string) string {
				return queue
			},
			Durable: true,
		},
	)
}
