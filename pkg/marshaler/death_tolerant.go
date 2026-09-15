package marshaler

import (
	"strconv"

	wamqp "github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	stdamqp "github.com/rabbitmq/amqp091-go"
)

const (
	// xDeathHeaderKey - заголовок, который RabbitMQ сам добавляет к сообщению при возврате
	// через Dead Letter Exchange. Не строка ([]interface{} с amqp.Table внутри) — константы для
	// него нет ни в amqp091-go, ни в watermill-amqp.
	xDeathHeaderKey = "x-death"

	// xDeathCountFieldKey - поле внутри первой записи x-death со счётчиком попыток.
	xDeathCountFieldKey = "count"

	// deathCountMetadataKey - под этим ключом в message.Metadata оказывается счётчик попыток из
	// заголовка x-death, если он присутствует.
	deathCountMetadataKey = "x-death-count"
)

// DeathTolerantMarshaler оборачивает amqp.DefaultMarshaler и не роняет разбор сообщения на
// нестроковых AMQP-заголовках.
//
// wamqp.DefaultMarshaler.Unmarshal требует, чтобы КАЖДЫЙ заголовок был строкой, и возвращает
// ошибку на первом же нестроковом значении. RabbitMQ сам добавляет x-death
// ([]interface{}{amqp.Table{...}}) при возврате сообщения через Dead Letter Exchange — без этой
// обёртки любое сообщение, прошедшее через DLX-retry (TTL-очередь, dead-lettering и т.п.),
// гарантированно проваливает разбор и уходит в бесконечный Nack-цикл.
//
// Остальные нестроковые заголовки (кроме x-death) просто отбрасываются из Metadata. Используйте
// DeathTolerantMarshaler вместо DefaultMarshaler в любом consumer-е, чья очередь настроена с
// x-dead-letter-exchange или получает сообщения из retry-топологии.
type DeathTolerantMarshaler struct {
	wamqp.DefaultMarshaler
}

func (m DeathTolerantMarshaler) Unmarshal(amqpMsg stdamqp.Delivery) (*message.Message, error) {
	filtered := amqpMsg
	filtered.Headers = make(stdamqp.Table, len(amqpMsg.Headers))

	for key, value := range amqpMsg.Headers {
		if strValue, ok := value.(string); ok {
			filtered.Headers[key] = strValue
		}
	}

	if count, ok := deathCount(amqpMsg.Headers); ok {
		filtered.Headers[deathCountMetadataKey] = strconv.FormatInt(count, 10)
	}

	return m.DefaultMarshaler.Unmarshal(filtered)
}

// deathCount читает count из первой записи x-death (самая свежая попытка).
func deathCount(headers stdamqp.Table) (int64, bool) {
	deaths, ok := headers[xDeathHeaderKey].([]interface{})
	if !ok || len(deaths) == 0 {
		return 0, false
	}

	table, ok := deaths[0].(stdamqp.Table)
	if !ok {
		return 0, false
	}

	count, ok := table[xDeathCountFieldKey].(int64)

	return count, ok
}
