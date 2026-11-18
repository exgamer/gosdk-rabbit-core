# gosdk-rabbitmq-core

`gosdk-rabbitmq-core` — HTTP kernel для работы с rabbitmq, построенных на базе `gosdk-core`.
Пакет инкапсулирует инициализацию консьюмера и паблишера rabbitmq и подключается к приложению как **kernel**.

Основная цель — дать единый и предсказуемый способ поднятия rabbitmq слоя поверх core SDK.

---

## 📦 Возможности

- 🌐 rabbitmq kernel для `gosdk-core`
- 🚀 Инициализация rabbitmq consumer 
- 🚀 Инициализация rabbitmq publisher
- ⚙️ Конфигурация через config
- ⏱️ Отложенные сообщения (`PublishDelayed`, TTL + DLX)
- 🔁 Повторы с паузой при ошибке обработчика и парковочная очередь `<queue>.dlq`
- ♻️ Корректный shutdown
- 🧠 Интеграция с DI контейнером

---

## 🚀 Установка

```bash
go get github.com/exgamer/gosdk-rabbitmq-core
```

---

[Что доступно в DI из коробки](pkg/di/container.go)

---

## 🧠 Концепция HTTP Kernel

HTTP kernel — это kernel приложения, который:
- регистрирует rabbitmq зависимости в DI
- инициализирует rabbitmq consumer
- инициализирует rabbitmq publisher
- корректно завершает работу при shutdown

Kernel реализует интерфейс `KernelInterface` из `gosdk-core`.

---

## 🔌 Регистрация RabbitMq Kernel

```go
app.RegisterKernel(app.NewRabbitKernel().EnableConsumer().EnablePublisher()) // включаем консьюмер или паблишер в зависимости от необходимости
```

---

## ⚙️ Конфигурация

HTTP kernel использует конфигурацию из `pkg/config`.

Пример env-переменных:

```env
RABBITMQ_HOST=127.0.0.1
RABBITMQ_PORT=5672
RABBITMQ_VHOST='/'
RABBITMQ_USER=rabbitmq
RABBITMQ_PASSWORD=rabbitmq
```

Переменные `RABBITMQ_*` (`config.RabbitConfig`) читаются из окружения процесса и из `.env`;
окружение главнее `.env`, пустая переменная в окружении перекрывает значение из `.env`.

Логин и пароль в URI экранируются: спецсимволы (`% # / @ : ?`) в `RABBITMQ_PASSWORD` допустимы.
`RABBITMQ_VHOST`: `/` или пусто - vhost по умолчанию, `/main` или `main` - vhost `main`.
Пароль не печатается: `String`, `%#v` и JSON конфига выводят `***`, ошибка подключения - без пароля.

---

## 🔧 Кастомный Marshaler и Connection

- `config.WithCustomMarshaler(m amqp.Marshaler)` — свой `Marshaler` (в отличие от `WithMarshaller`, принимающего только конкретный `amqp.DefaultMarshaler`).
- `config.WithConnectionConfig(cc amqp.ConnectionConfig)` — полная замена `ConnectionConfig` (TLS, raw `amqp091-go` конфиг, реконнект).
- `marshaler.DeathTolerantMarshaler` (`pkg/marshaler`) — обёртка над `DefaultMarshaler`, не падает на нестроковых заголовках (например `x-death` от Dead Letter Exchange). Использовать через `config.WithCustomMarshaler(marshaler.DeathTolerantMarshaler{})` в любом consumer-е с retry-топологией через DLX.

---

## 🗺️ Декларация топологии без consumer-а

Для очередей, у которых намеренно нет читателя в этом сервисе (TTL/DLX retry-очередь и т.п.), не нужно поднимать отдельное `amqp.Dial`-соединение в обход SDK:

```go
conn, err := rabbitDi.GetRabbitConnection(a.Container)
if err != nil {
    return err
}

err = rabbitmq.DeclareTopology(conn, func(ch *amqp091.Channel) error {
    if err := ch.ExchangeDeclare("outbox-retry", "direct", true, false, false, false, nil); err != nil {
        return err
    }

    _, err := ch.QueueDeclare("outbox-retry-tier-1", true, false, false, false, amqp091.Table{
        "x-message-ttl":          10_000,
        "x-dead-letter-exchange": "events",
    })

    return err
})
```

`rabbitDi.GetRabbitConnection` отдаёт то же самое переиспользуемое соединение kernel-а (с реконнектом), что используется под consumer/publisher — отдельно поднимать и закрывать своё соединение больше не нужно.

---

## 🧩 Работа с консьюмером

[Работа с консьюмером](/pkg/rabbitmq/CONSUMERREADME.md)

## 🔁 Повторы и `.dlq`

Ошибка или паника обработчика - сообщение возвращается в свою очередь с паузой
(5 с, 30 с, 2 мин, 10 мин, 30 мин; счётчик - заголовок `x-sdk-attempt`) через `<queue>.retry.<пауза>`,
после пятого повтора - в парковочную очередь `<queue>.dlq`. У очереди со своим `x-dead-letter-exchange`
в аргументах повторов SDK нет - Nack, дальше политика DLX сервиса (`NoRequeueOnNack` из его конфига). Нестроковые заголовки в копии
не переносятся; доставка at-least-once - обработчик должен быть идемпотентным. Подробно - [повторы и `.dlq`](/pkg/rabbitmq/CONSUMERREADME.md#ошибки-обработчика-повторы-и-dlq).

## ⏱️ Отложенные сообщения

```go
pub.PublishDelayed("orders.remind", payload, 30*time.Second)
```

Сообщение уходит в очередь задержки `<exchange>.delay.30s` (TTL) и по истечении приходит в exchange
публикатора с исходным routing key. Каждая уникальная задержка - своя очередь: держите фиксированный
набор значений. Подробно - [отложенные сообщения](/pkg/rabbitmq/PUBLISHERREADME.md#отложенные-сообщения).


## 🧩 Работа с паблишером

---
[Работа с паблишером](/pkg/rabbitmq/PUBLISHERREADME.md)

---

## ♻️ Graceful Shutdown

RabbitMq kernel автоматически:
- прекращает приём сообщений и дожидается текущих обработчиков (в пределах таймаута остановки)
- закрывает подписчиков и publisher-ы
- завершает соединение
- при падении `Consume` (ошибка подписки, закрытое соединение) останавливает приложение через `App.Fail`
---

## 📌 Используется вместе с

- `gosdk-core`
- internal business modules

---

## 📝 License

MIT или внутренняя лицензия компании
