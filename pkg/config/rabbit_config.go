package config

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strings"
)

// passwordMask - то, что печатается вместо пароля в String, GoString и JSON.
const passwordMask = "***"

// RabbitConfig Данные для соединения с RabbitMQ
type RabbitConfig struct {
	Host  string `mapstructure:"RABBITMQ_HOST" json:"RABBITMQ_HOST"`
	Port  string `mapstructure:"RABBITMQ_PORT" json:"RABBITMQ_PORT"`
	User  string `mapstructure:"RABBITMQ_USER" json:"RABBITMQ_USER"`
	Pass  string `mapstructure:"RABBITMQ_PASSWORD" json:"RABBITMQ_PASSWORD"`
	VHost string `mapstructure:"RABBITMQ_VHOST" json:"RABBITMQ_VHOST"`
}

// AmqpURI - собирает connection string в формате amqp://user:pass@host:port/vhost.
// Логин и пароль экранируются (символы % # / @ : ? в пароле не ломают разбор), хост IPv6
// берётся в скобки. VHost - как и раньше, путь URI: "" и "/" - vhost по умолчанию "/",
// "/main" и "main" - vhost "main"; уже экранированные последовательности (%2F) сохраняют смысл.
func (c RabbitConfig) AmqpURI() string {
	u := url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(c.User, c.Pass),
		Host:   net.JoinHostPort(c.Host, c.Port),
	}
	setVHostPath(&u, c.VHost)

	return u.String()
}

// setVHostPath Кладёт vhost в путь URI одним сегментом: "/" внутри имени экранируется как %2F,
// чтобы amqp091 получил имя vhost целиком.
func setVHostPath(u *url.URL, vhost string) {
	if vhost == "" {
		return
	}

	raw := vhost
	if !strings.HasPrefix(raw, "/") {
		raw = "/" + raw
	}

	decoded, err := url.PathUnescape(raw)
	if err != nil {
		// Одиночный % без двух hex-цифр - буквальный символ имени.
		decoded = raw
	}

	name := decoded[1:]
	if name == "" {
		u.Path = "/"

		return
	}

	u.Path = "/" + name
	u.RawPath = "/" + url.PathEscape(name)
}

// String Конфиг с маской вместо пароля (fmt %v, %+v, %s).
func (c RabbitConfig) String() string {
	return fmt.Sprintf("{Host:%s Port:%s User:%s Pass:%s VHost:%s}", c.Host, c.Port, c.User, c.maskedPass(), c.VHost)
}

// GoString Конфиг с маской вместо пароля (fmt %#v).
func (c RabbitConfig) GoString() string {
	return fmt.Sprintf("config.RabbitConfig{Host:%q, Port:%q, User:%q, Pass:%q, VHost:%q}",
		c.Host, c.Port, c.User, c.maskedPass(), c.VHost)
}

// MarshalJSON Конфиг в JSON с маской вместо пароля (дамп конфига, лог структурой).
// Чтение конфига (InitConfig) это не затрагивает: оно только разбирает JSON.
func (c RabbitConfig) MarshalJSON() ([]byte, error) {
	type plain RabbitConfig

	masked := plain(c)
	masked.Pass = c.maskedPass()

	return json.Marshal(masked)
}

func (c RabbitConfig) maskedPass() string {
	if c.Pass == "" {
		return ""
	}

	return passwordMask
}
