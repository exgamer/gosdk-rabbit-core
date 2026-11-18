package rabbitmq

import (
	"errors"
	"net/url"
	"strings"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/exgamer/gosdk-core/pkg/logger"
)

// uriPasswordMask - то, что подставляется вместо пароля в тексте ошибки подключения.
const uriPasswordMask = "***"

// NewAmqpConnection Соединение с RabbitMQ. Текст ошибки подключения не содержит пароль из AmqpURI.
func NewAmqpConnection(cfg amqp.ConnectionConfig) (*amqp.ConnectionWrapper, error) {
	conn, err := amqp.NewConnection(cfg, newWatermillLogger())
	if err != nil {
		return nil, errors.New(redactURIPassword(err.Error(), cfg.AmqpURI))
	}

	return conn, nil
}

// newWatermillLogger Логгер watermill с уровнем логгера SDK: debug/trace - только если они включены.
func newWatermillLogger() watermill.LoggerAdapter {
	return watermill.NewStdLogger(logger.IsDebugLevel(), logger.IsTraceLevel())
}

// redactURIPassword Заменяет пароль из uri (в экранированном и в исходном виде) маской в тексте text.
func redactURIPassword(text, uri string) string {
	pass := uriPassword(uri)
	if pass == "" {
		return text
	}

	text = strings.ReplaceAll(text, pass, uriPasswordMask)

	if decoded, err := url.PathUnescape(pass); err == nil && decoded != "" {
		text = strings.ReplaceAll(text, decoded, uriPasswordMask)
	}

	return text
}

// uriPassword Пароль из userinfo uri в том виде, в каком он записан в строке.
// Не использует url.Parse: строка с ошибкой (например, нечисловой порт) тоже должна маскироваться.
func uriPassword(uri string) string {
	_, rest, ok := strings.Cut(uri, "://")
	if !ok {
		return ""
	}

	if end := strings.IndexAny(rest, "/?#"); end >= 0 {
		rest = rest[:end]
	}

	at := strings.LastIndex(rest, "@")
	if at < 0 {
		return ""
	}

	_, pass, ok := strings.Cut(rest[:at], ":")
	if !ok {
		return ""
	}

	return pass
}
