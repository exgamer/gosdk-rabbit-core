# RabbitMQ Publisher (Watermill AMQP)

Этот пакет предоставляет **долгоживущий** RabbitMQ publisher поверх **общего AMQP connection** (Watermill AMQP), а также **реестр паблишеров**, чтобы модули могли удобно объявлять свои паблишеры, а `RabbitKernel` — инициализировать их на старте.

---

## Зачем так сделано

### ✅ Правильный lifecycle
- AMQP connection создаётся **один раз** в `RabbitKernel`.
- Каждый publisher создаётся **один раз** и переиспользуется на протяжении жизни приложения.
- На `Stop()` все publisher’ы корректно закрываются, затем закрывается connection.

### ❌ Неправильный вариант
Создавать `amqp.NewPublisherWithConnection(...)` на каждый `Publish()` — дорого и ломает смысл channel pool и confirm delivery.

---

## Термины: что такое `topic`

В Watermill `topic` — это логическое имя канала сообщений.  
В нашем SDK принято соглашение:

- Для `direct` и `topic` exchange:  
  **`topic` = routing key**
- Для `fanout` exchange:  
  routing key игнорируется

Пример:
```go
events.Publish("orders.paid", payload)
```

RabbitMQ:
```
exchange = "events"
routing_key = "orders.paid"
```

---

## Быстрый старт

Включить publisher в Kernel:

```go
rabbitKernel := app.NewRabbitKernel().
    EnablePublisher()
```

---

## Объявление паблишеров

```go
pubReg.Register(config.NewPublisherDefinition(
    "events",
    config.NewPublisherTopicDurableConfig("events")...,
))

pubReg.Register(config.NewPublisherDefinition(
    "broadcast",
    config.NewPublisherFanoutDurableConfig("broadcast")...,
))
```

- `events`, `broadcast` — имена паблишеров в registry
- `"events"`, `"broadcast"` — RabbitMQ exchange

---

## Использование

```go
eventsPub, err := pubReg.Get("events")
if err != nil {
    return err
}

return eventsPub.Publish("orders.paid", payload)
```

С метаданными:

```go
eventsPub.PublishWithMetaData("orders.paid", map[string]string{
    "trace_id": traceID,
}, payload)
```

Batch:

```go
eventsPub.PublishBatch("products.updated", []any{p1, p2, p3})
```

С ограничением по времени (`ctx` ограничивает отправку и ожидание подтверждения брокера;
если `ctx` истёк раньше подтверждения - ошибка, но сообщение могло дойти):

```go
eventsPub.PublishCtx(ctx, "orders.paid", payload)
```

---

## Отложенные сообщения

```go
// придёт в exchange "events" с routing key "orders.remind" не раньше чем через 30 секунд
eventsPub.PublishDelayed("orders.remind", payload, 30*time.Second)
```

Схема (на соединении kernel-а, объявляется лениво, один раз на пару exchange + задержка):

```
publish(routing key = K)
  -> exchange  events.delay.30s   (fanout, durable)
  -> queue     events.delay.30s   (durable, x-message-ttl=30000, x-dead-letter-exchange=events)
  -> через 30 с: dead-letter в exchange events с тем же routing key K
  -> очереди, привязанные к events по K (direct / topic / fanout - как обычная публикация)
```

- `x-dead-letter-routing-key` у очереди задержки не задаётся - RabbitMQ отправляет сообщение
  с исходным routing key, поэтому одна очередь задержки обслуживает все ключи exchange-а.
- Задержка - от `MinPublishDelay` (1 с) до `MaxPublishDelay` (24 ч), доли секунды округляются вверх.
- **Каждая уникальная задержка - своя очередь** (`events.delay.30s`, `events.delay.2m`, ...).
  Используйте фиксированный набор значений, а не вычисленные «до 17:00» задержки.
- Публикация persistent, с подтверждением брокера; целевой exchange объявляется заранее
  (иначе брокер выбросил бы сообщение по истечении TTL).
- Получатель видит заголовок `x-death`: консьюмеры SDK разбирают его (`DeathTolerantMarshaler`).
- Для публикатора без exchange (публикация в очередь через exchange по умолчанию) очередь задержки
  называется `sdk.default.delay.<задержка>`.

---

## Id корреляции

Методы `...Ctx` (`PublishCtx`, `PublishWithMetaDataCtx`, `PublishDelayedCtx` и др.) кладут сквозной id корреляции
`ctx` (`sdkctx.CorrelationID(ctx)`, gosdk-core; в HTTP-обработчике — id запроса из http-core) в метаданные
`correlation_id` (`rabbitmq.CorrelationIDMetadataKey`). Свой `correlation_id` в `meta` главнее; нет id в `ctx` —
метаданных нет. Методы без `ctx` id не ставят: публикуйте из обработчика через `...Ctx(ctx, ...)`.

```go
return events.PublishCtx(c.Request.Context(), "orders.paid", payload)
```

---

## Durable delivery

`*DurableConfig` означает:
- Exchange с `Durable=true`
- Сообщения публикуются как **persistent**

Это гарантирует сохранность сообщений при рестарте RabbitMQ.

---

## API

### Publisher
- `Publish(topic string, payload any) error`
- `PublishWithMetaData(topic string, meta map[string]string, payload any) error`
- `PublishBatch(topic string, payloads []any) error`
- `PublishBatchWithMetaData(topic string, meta map[string]string, payloads []any) error`
- `PublishCtx`, `PublishWithMetaDataCtx`, `PublishBatchCtx`, `PublishBatchWithMetaDataCtx` - то же с `ctx`
- `PublishDelayed(topic string, payload any, delay time.Duration) error`
- `PublishDelayedWithMetaData(topic string, meta map[string]string, payload any, delay time.Duration) error`
- `PublishBatchDelayed`, `PublishBatchDelayedWithMetaData` - отложенный batch
- `PublishDelayedCtx`, `PublishDelayedWithMetaDataCtx`, `PublishBatchDelayedCtx`, `PublishBatchDelayedWithMetaDataCtx` - то же с `ctx`
- `Close() error`

### PublisherRegistry
- `Register(def PublisherDefinition) error`
- `RegisterMultiple(list []PublisherDefinition) error`
- `Init(conn *amqp.ConnectionWrapper) error`
- `Get(name string) (*Publisher, error)`
- `Close() error`

---

## Рекомендации по неймингу routing keys

Используйте формат:

```
domain.event
```

Примеры:
- `orders.paid`
- `orders.created`
- `products.updated`
- `users.created`

Позволяет подписываться масками:
- `orders.*`
- `*.updated`
