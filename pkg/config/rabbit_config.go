package config

import "fmt"

type RabbitConfig struct {
	Host  string `mapstructure:"RABBITMQ_HOST"`
	Port  string `mapstructure:"RABBITMQ_PORT"`
	User  string `mapstructure:"RABBITMQ_USER"`
	Pass  string `mapstructure:"RABBITMQ_PASSWORD"`
	VHost string `mapstructure:"RABBITMQ_VHOST"`
}

// AmqpURI - собирает connection string в формате amqp://user:pass@host:port/vhost.
func (c RabbitConfig) AmqpURI() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%s%s", c.User, c.Pass, c.Host, c.Port, c.VHost)
}
