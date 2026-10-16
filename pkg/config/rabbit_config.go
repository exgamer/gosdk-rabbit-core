package config

import "fmt"

// RabbitConfig Данные для соединения с RabbitMQ
type RabbitConfig struct {
	Host  string `mapstructure:"RABBITMQ_HOST" json:"RABBITMQ_HOST"`
	Port  string `mapstructure:"RABBITMQ_PORT" json:"RABBITMQ_PORT"`
	User  string `mapstructure:"RABBITMQ_USER" json:"RABBITMQ_USER"`
	Pass  string `mapstructure:"RABBITMQ_PASSWORD" json:"RABBITMQ_PASSWORD"`
	VHost string `mapstructure:"RABBITMQ_VHOST" json:"RABBITMQ_VHOST"`
}

// AmqpURI - собирает connection string в формате amqp://user:pass@host:port/vhost.
func (c RabbitConfig) AmqpURI() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s%s", c.User, c.Pass, c.Host, c.Port, c.VHost)
}
