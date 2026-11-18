package rabbitmq

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	sdkctx "github.com/exgamer/gosdk-core/pkg/context"
)

// CorrelationIDMetadataKey Метаданные (заголовок AMQP) сообщения со сквозным id корреляции.
const CorrelationIDMetadataKey = "correlation_id"

// setCorrelationID Ставит id корреляции ctx в метаданные сообщений, где своего нет: явная метадата вызывающего главнее.
// Нет id в ctx — метаданные не меняются.
func setCorrelationID(ctx context.Context, msgs []*message.Message) {
	id := sdkctx.CorrelationID(ctx)
	if id == "" {
		return
	}

	for _, msg := range msgs {
		if msg.Metadata == nil {
			msg.Metadata = make(message.Metadata)
		}

		if msg.Metadata.Get(CorrelationIDMetadataKey) == "" {
			msg.Metadata.Set(CorrelationIDMetadataKey, id)
		}
	}
}

// consumeCorrelation ctx обработчика с id корреляции сообщения. Нет id в метаданных — новый; он записывается
// в метаданные сообщения, чтобы повтор и .dlq несли тот же id. Контекст сообщения (msg.Context()) получает тот же id.
func consumeCorrelation(ctx context.Context, msg *message.Message) context.Context {
	if msg.Metadata == nil {
		msg.Metadata = make(message.Metadata)
	}

	id := msg.Metadata.Get(CorrelationIDMetadataKey)
	if id == "" {
		id = sdkctx.NewCorrelationID()
		msg.Metadata.Set(CorrelationIDMetadataKey, id)
	}

	msg.SetContext(sdkctx.WithCorrelationID(msg.Context(), id))

	return sdkctx.WithCorrelationID(ctx, id)
}
