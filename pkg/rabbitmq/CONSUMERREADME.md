## 🧩 Работа с консьюмером

Консьюмеры регистрируются в DI и доступен в бизнес-модулях.

```go
package consumers

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill/message"
)

func NewCityConsumer() *CityConsumer {
	return &CityConsumer{}
}

type CityConsumer struct {
}

func (c *CityConsumer) Consume(ctx context.Context, msg *message.Message) error {
	fmt.Println(string(msg.Payload))

	return nil
}
```

```go
package city

import (
	"github.com/exgamer/gosdk-rabbitmq-consumer-template/internal/domains/handbbok/modules/city/factories"
	"github.com/exgamer/gosdk-rabbitmq-consumer-template/pkg/config"
)

func GetConsumers(
	consumersFactory *factories.CityConsumersFactory,
) []config.HandlerRegister {
	return []config.HandlerRegister{
		{
			Handler: consumersFactory.CityConsumer.Consume,
			Config: config.NewConsumerTopicDurableConfig(
				"test-rk",
				"test",
				"test",
				100,
			),
		},
	}
}

```

```go
consumersFactory := factories.NewCityConsumersFactory()

consumers := GetConsumers(consumersFactory)

reg, err := di.GetRabbitConsumersRegistry(a.Container) // твой helper для DI
if err != nil {
    return err
}

reg.RegisterMultipleHandler(consumers)
```

---

## Ошибки обработчика: повторы и `.dlq`

По умолчанию (`ActionRetry`) ошибка или паника обработчика не возвращает сообщение в очередь
бесконечно:

1. Копия сообщения публикуется (с подтверждением брокера) в очередь задержки
   `<queue>.retry.<пауза>` (durable, `x-message-ttl`, `x-dead-letter-exchange=""`,
   `x-dead-letter-routing-key=<queue>`), исходное сообщение подтверждается (Ack).
   По истечении паузы копия возвращается прямо в `<queue>` (другие очереди exchange-а её не получают).
2. Паузы: 5 с, 30 с, 2 мин, 10 мин, 30 мин - всего 5 повторов (первая обработка + 5).
   Счётчик - заголовок `x-sdk-attempt` (метаданные `msg.Metadata`).
3. После последнего повтора сообщение публикуется в парковочную очередь `<queue>.dlq`
   (SDK объявляет её сам, durable; уже существующая берётся как есть) с заголовками
   `x-sdk-attempt`, `x-sdk-original-queue`, `x-sdk-last-error`, затем Ack.
4. Если у очереди в конфиге свой `x-dead-letter-exchange` (`amqp.QueueConfig.Arguments`) - своя
   политика DLX сервиса не трогается: повторов, копий и очередей SDK нет, ошибка или паника - обычный
   Nack исходного. Дальше решает `NoRequeueOnNack` из конфига сервиса: `true` - брокер уводит
   сообщение в DLX с заголовком `x-death` (счётчик кругов - `x-death-count` у `DeathTolerantMarshaler`),
   `false` - возврат в очередь. SDK этот флаг не меняет.
5. Если копию опубликовать не удалось (брокер недоступен) - Nack: сообщение не теряется.

При остановке watermill отклоняет полученные, но ещё не отданные обработчику сообщения через Nack:
без `NoRequeueOnNack` они остаются в очереди, с ним - уходят по политике DLX сервиса.

Копии (`<queue>.retry.*`, `<queue>.dlq`) собираются из `msg.Metadata`: нестроковые заголовки
исходного сообщения (числа, таблицы, `x-death`) в них не попадают.

Доставка at-least-once: если соединение оборвалось между подтверждением копии брокером и Ack
исходного, сообщение придёт ещё раз (дубль). Обработчик должен быть идемпотентным.

Сообщение из `.dlq` разбирают вручную или переотправляют в исходную очередь (shovel, UI RabbitMQ).
Политика DLX, заданная на брокере (policy), а не в аргументах очереди, SDK не видна.

Явно заданное поведение сохраняется:

```go
consumer.WithErrorAction(rabbitmq.ActionAck)  // ошибка - Ack, сообщение теряется
consumer.WithPanicAction(rabbitmq.ActionNack) // паника - Nack (requeue, если сервис не включил NoRequeueOnNack)
```

`amqp.DefaultMarshaler` в конфиге подписчика заменяется на `marshaler.DeathTolerantMarshaler`
с теми же настройками (свой marshaler сервиса не заменяется): вернувшиеся через DLX сообщения несут заголовок `x-death`.

## Id корреляции

`ctx` обработчика несёт сквозной id корреляции (`sdkctx.CorrelationID(ctx)`, gosdk-core) из метаданных
`correlation_id` сообщения; нет — новый id, он же записывается в метаданные сообщения, так что повторы и `.dlq` несут
тот же id. Публикации обработчика через `...Ctx(ctx, ...)` передают id дальше; события Sentry получают тег
`correlation_id`.

## Остановка

`RabbitKernel.Stop` (и `Consumer.Close(ctx)`): приём прекращается, текущие обработчики
дорабатывают - их `ctx` остановкой не отменяется, - в пределах `ctx` остановки; затем закрываются
подписчики, publisher-ы и соединение. Не успевший обработчик - его сообщение брокер вернёт в очередь.

Если `Consume` завершился сам (ошибка подписки, закрытое соединение) - kernel пишет ERROR
и останавливает приложение через `App.Fail`.

## Error-трекер (Sentry)

Отправляются: handler, panic, id и размер сообщения, очередь, exchange, ключ привязки, номер повтора.
Payload - только при явном `WithSentryPayloadLimit(n)` с `n > 0` (первые `n` байт), заголовки - никогда.
