//go:build integration && !race

package rabbitmq

// Отдельный файл без -race: закрытие соединения посреди подписки задевает гонку внутри
// watermill-amqp v2.1.3 (ConnectionWrapper.handleConnectionClose против Subscriber), не в коде SDK.
// Запуск: RABBIT_IT_URI=... go test -tags integration -count=1 ./pkg/rabbitmq/

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/exgamer/gosdk-rabbit-core/pkg/config"
)

// TestIntegration_ConsumeFailsWhenConnectionClosed Соединение закрыто не через Close консьюмера -
// Consume возвращает ошибку, а не висит без приёма сообщений.
func TestIntegration_ConsumeFailsWhenConnectionClosed(t *testing.T) {
	conn, err := NewAmqpConnection(amqp.ConnectionConfig{AmqpURI: itURI(t)})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	exchange := itName("dead")

	_, done := startConsumer(t, conn, func(context.Context, *message.Message) error { return nil },
		config.NewConsumerDirectDurableConfig("it", "k", exchange, exchange+".q", 1))

	time.Sleep(500 * time.Millisecond)

	_ = conn.Close()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Consume returned nil after the connection was closed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Consume did not return")
	}
}
