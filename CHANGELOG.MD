# Changelog

## v1.0.12

`RABBITMQ_*` из переменных окружения процесса (Docker, k8s, `docker run -e`) читаются
и без `.env`. Типы, поля и сигнатуры не изменились.

### Изменено
- `gosdk-core` поднят до v1.0.28: `logger.Error` пишет на уровне Error.

### Исправлено
- У полей `config.RabbitConfig` есть `json`-теги с теми же именами, что `mapstructure`
  (`json:"RABBITMQ_HOST"` и т. д.). Без них `InitConfig` из `gosdk-core` находил переменную
  в окружении, но не мог положить её в поле: применялись только ключи, которые есть в `.env`.
  Требуется `gosdk-core` v1.0.27+ (в `go.mod` - v1.0.28).

### ⚠️ Изменения поведения
- Сообщения `logger.Error` видны на уровнях info и выше с `[ERROR]` (раньше писались
  на уровне Debug и на рабочих уровнях не выводились): `app.RabbitKernel` - «rabbit consumer stopped»,
  consumer `rabbitmq` - «failed to register amqp handler», «failed to subscribe to queue».
- Переменная `RABBITMQ_*`, которой нет в `.env`, но которая задана в окружении процесса,
  теперь применяется (раньше молча игнорировалась, поле оставалось пустым). Если `.env` нет совсем,
  конфиг целиком берётся из окружения. Для ключей, которые есть в `.env`, окружение было главнее
  и раньше.
- Пустая переменная в окружении (`RABBITMQ_PASSWORD=`) теперь перекрывает значение из `.env`
  пустой строкой (раньше бралось значение из `.env`). Не задавайте пустые `RABBITMQ_*`
  в окружении, если значение должно прийти из `.env`.
- `json.Marshal(RabbitConfig)` выдаёт ключи `RABBITMQ_*` вместо имён полей (`Host`, `Pass`…).
  В SDK, `go-sdk-rest-template` и `gosdk-rabbitmq-consumer-template` конфиг в JSON не сериализуется.
- Затронуты все места, где конфиг читается через `InitRabbitConfig`: `app.RabbitKernel`
  (`Init`) и сервисы, которые подключают этот kernel.
