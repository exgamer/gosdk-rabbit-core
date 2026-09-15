package rabbitmq

import (
	stdamqp "github.com/rabbitmq/amqp091-go"

	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
)

// DeclareTopology открывает канал на уже существующем kernel-соединении (di.GetRabbitConnection)
// и выполняет в нём declare — без создания собственного consumer-а или publisher-а.
//
// Нужен для очередей, у которых намеренно нет читателя в этом сервисе (например TTL/DLX
// retry-очередь: сообщения сами возвращаются в основную очередь по истечении TTL). До появления
// этой функции для такого declare приходилось поднимать отдельное amqp.Dial-соединение в обход
// SDK, дублируя сборку connection string и жизненный цикл соединения.
//
// Канал закрывается автоматически перед возвратом, независимо от результата declare.
func DeclareTopology(conn *amqp.ConnectionWrapper, declare func(ch *stdamqp.Channel) error) error {
	ch, err := conn.Connection().Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return declare(ch)
}
