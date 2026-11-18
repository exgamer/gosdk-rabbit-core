package rabbitmq

import (
	"fmt"
	"strconv"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	stdamqp "github.com/rabbitmq/amqp091-go"
)

const (
	// MinPublishDelay - наименьшая задержка отложенного сообщения.
	MinPublishDelay = time.Second
	// MaxPublishDelay - наибольшая задержка отложенного сообщения.
	MaxPublishDelay = 24 * time.Hour

	// AttemptHeader - заголовок со счётчиком повторов SDK: сколько раз сообщение уже
	// возвращалось в очередь после ошибки обработчика.
	AttemptHeader = "x-sdk-attempt"
	// LastErrorHeader - текст последней ошибки обработчика у сообщения в парковочной очереди.
	LastErrorHeader = "x-sdk-last-error"
	// OriginalQueueHeader - очередь, из которой сообщение ушло в парковочную очередь.
	OriginalQueueHeader = "x-sdk-original-queue"

	// ParkingQueueSuffix - суффикс парковочной очереди: <queue>.dlq.
	ParkingQueueSuffix = ".dlq"

	// lastErrorMaxLen - сколько байт текста ошибки кладётся в LastErrorHeader.
	lastErrorMaxLen = 1024
	// deathCountMetadataKey - служебный ключ marshaler.DeathTolerantMarshaler, не переносится при повторе.
	deathCountMetadataKey = "x-death-count"
	// republishTimeout - сколько ждать подтверждения брокера при повторе и парковке.
	republishTimeout = 30 * time.Second
	// defaultExchangeDelayPrefix - префикс имени очереди задержки для публикации в exchange по умолчанию ("").
	defaultExchangeDelayPrefix = "sdk.default"
)

// retryDelays - паузы перед повторами сообщения, на котором обработчик вернул ошибку или
// запаниковал: после i-й неудачи - retryDelays[i-1]. Число ступеней - число повторов;
// после последней неудачи сообщение уходит в <queue>.dlq. У очереди со своим DLX ступени не применяются.
// Переменная, а не константа - только чтобы интеграционный тест подставил короткие паузы.
var retryDelays = []time.Duration{
	5 * time.Second,
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
	30 * time.Minute,
}

// normalizeDelay Проверяет задержку и округляет её вверх до целых секунд.
func normalizeDelay(d time.Duration) (time.Duration, error) {
	if d < MinPublishDelay || d > MaxPublishDelay {
		return 0, fmt.Errorf("delay %s out of range [%s, %s]", d, MinPublishDelay, MaxPublishDelay)
	}

	rounded := d.Truncate(time.Second)
	if rounded < d {
		rounded += time.Second
	}

	if rounded > MaxPublishDelay {
		return 0, fmt.Errorf("delay %s out of range [%s, %s]", d, MinPublishDelay, MaxPublishDelay)
	}

	return rounded, nil
}

// delaySuffix Короткая запись целых секунд: 30s, 2m, 1h, 90s, 3700s.
func delaySuffix(d time.Duration) string {
	secs := int64(d / time.Second)

	switch {
	case secs%3600 == 0:
		return strconv.FormatInt(secs/3600, 10) + "h"
	case secs%60 == 0:
		return strconv.FormatInt(secs/60, 10) + "m"
	default:
		return strconv.FormatInt(secs, 10) + "s"
	}
}

// delayQueueName Очередь (и fanout-exchange) задержки для отложенной публикации в exchange.
func delayQueueName(exchange string, d time.Duration) string {
	if exchange == "" {
		exchange = defaultExchangeDelayPrefix
	}

	return exchange + ".delay." + delaySuffix(d)
}

// retryQueueName Очередь задержки повторов очереди queue.
func retryQueueName(queue string, d time.Duration) string {
	return queue + ".retry." + delaySuffix(d)
}

// parkingQueueName Парковочная очередь очереди queue.
func parkingQueueName(queue string) string {
	return queue + ParkingQueueSuffix
}

// delayQueueArgs Аргументы очереди задержки: TTL и куда сообщение уходит по истечении.
// Без routingKey сообщение уходит с исходными ключами маршрутизации.
func delayQueueArgs(d time.Duration, deadLetterExchange string, routingKey *string) stdamqp.Table {
	args := stdamqp.Table{
		"x-message-ttl":          d.Milliseconds(),
		"x-dead-letter-exchange": deadLetterExchange,
	}

	if routingKey != nil {
		args["x-dead-letter-routing-key"] = *routingKey
	}

	return args
}

// failureKind Что сделать с сообщением после ошибки обработчика (политика ActionRetry).
type failureKind int

const (
	// failureRetry - в очередь повторов с паузой.
	failureRetry failureKind = iota
	// failurePark - в <queue>.dlq.
	failurePark
	// failureServiceDLX - у очереди свой DLX: Nack без повторов SDK, дальше - политика сервиса.
	failureServiceDLX
	// failureNack - повтор невозможен (у очереди нет имени): Nack по настройке подписчика.
	failureNack
)

type failureDecision struct {
	kind  failureKind
	delay time.Duration
}

// decideFailure Решение по сообщению, которое уже повторялось attempt раз.
// У очереди со своим DLX - сразу Nack: политику повторов выбрал сервис.
func decideFailure(attempt int, queue string, ownDLX bool, delays []time.Duration) failureDecision {
	if ownDLX {
		return failureDecision{kind: failureServiceDLX}
	}

	if queue == "" {
		return failureDecision{kind: failureNack}
	}

	if attempt < len(delays) {
		return failureDecision{kind: failureRetry, delay: delays[attempt]}
	}

	return failureDecision{kind: failurePark}
}

// attemptOf Сколько раз сообщение уже повторялось (заголовок AttemptHeader); нет или мусор - 0.
func attemptOf(msg *message.Message) int {
	n, err := strconv.Atoi(msg.Metadata.Get(AttemptHeader))
	if err != nil || n < 0 {
		return 0
	}

	return n
}

// truncate Первые max байт s без разрыва UTF-8 символа.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}

	cut := max
	for cut > 0 && !utf8RuneStart(s[cut]) {
		cut--
	}

	return s[:cut]
}

func utf8RuneStart(b byte) bool {
	return b&0xC0 != 0x80
}
