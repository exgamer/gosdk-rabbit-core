package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/exgamer/gosdk-core/pkg/errorreporter"
	"github.com/exgamer/gosdk-core/pkg/logger"
	"github.com/exgamer/gosdk-rabbit-core/pkg/config"
	"github.com/exgamer/gosdk-rabbit-core/pkg/marshaler"
	stdamqp "github.com/rabbitmq/amqp091-go"
)

// Consumer — helper для запуска нескольких подписчиков (handlers).
type Consumer struct {
	conn *amqp.ConnectionWrapper

	mu       sync.Mutex
	handlers []*consumerEntry
	cancels  []context.CancelFunc
	closed   bool
	running  sync.WaitGroup // активные вызовы Consume (вместе с текущими обработчиками)

	logger watermill.LoggerAdapter

	// настройки поведения
	onError       ErrorAction
	onPanic       ErrorAction
	sentryPayload int // > 0 - сколько байт payload отправлять в error-трекер; 0 - не отправлять

	retry       *confirmPublisher // под mu: создаётся при первом повторе, закрывается в Close
	retryClosed bool              // под mu: Close закрыл публикатор повторов, новый не создаётся
}

// consumerEntry Подписчик и то, что SDK знает о его очереди (для повторов и парковки).
type consumerEntry struct {
	config.ConsumeConfig

	queue      string
	exchange   string
	routingKey string
	ownDLX     bool // у очереди в конфиге свой x-dead-letter-exchange
	marshaler  amqp.Marshaler
}

// ErrorAction определяет, что делать с сообщением при ошибке/панике.
type ErrorAction int

const (
	// ActionAck — подтвердить сообщение (сообщение будет потеряно, ретрая не будет)
	ActionAck ErrorAction = iota
	// ActionNack — отклонить сообщение: requeue (бесконечный повтор без паузы), если сервис
	// не включил NoRequeueOnNack у подписчика; SDK этот флаг сам не включает.
	ActionNack
	// ActionRetry — по умолчанию: повтор с растущей паузой (retryDelays: 5с, 30с, 2мин, 10мин, 30мин)
	// через очередь <queue>.retry.<пауза>, счётчик - заголовок x-sdk-attempt; после последнего
	// повтора - в парковочную очередь <queue>.dlq. Сообщение не теряется: если копия не опубликовалась - Nack.
	// У очереди со своим x-dead-letter-exchange повторов SDK нет: Nack исходного, дальше - политика
	// сервиса (NoRequeueOnNack из его конфига: DLX брокером с x-death или возврат в очередь).
	ActionRetry
)

func NewAmqpConsumer(conn *amqp.ConnectionWrapper) (*Consumer, error) {
	if conn == nil {
		return nil, errors.New("nil rabbit connection")
	}

	return &Consumer{
		conn:    conn,
		logger:  newWatermillLogger(),
		onError: ActionRetry,
		onPanic: ActionRetry,
	}, nil
}

// WithErrorAction — политика при ошибке handler-а (по умолчанию ActionRetry).
func (a *Consumer) WithErrorAction(action ErrorAction) *Consumer {
	a.onError = action

	return a
}

// WithPanicAction — политика при panic в handler-е (по умолчанию ActionRetry).
func (a *Consumer) WithPanicAction(action ErrorAction) *Consumer {
	a.onPanic = action

	return a
}

// WithSentryPayloadLimit — отправлять в error-трекер первые maxBytes байт payload сообщения.
// По умолчанию payload не отправляется (в нём могут быть персональные данные); 0 и меньше - не отправлять.
func (a *Consumer) WithSentryPayloadLimit(maxBytes int) *Consumer {
	a.sentryPayload = maxBytes

	return a
}

// RegisterMultipleHandler — регистрация набора консьюмеров.
func (a *Consumer) RegisterMultipleHandler(ctx context.Context, handlers []config.HandlerRegister) error {
	for _, h := range handlers {
		if err := a.RegisterHandler(ctx, h.Handler, h.Config...); err != nil {
			logger.Error(ctx, "failed to register amqp handler: "+err.Error())

			return err
		}
	}

	return nil
}

// RegisterHandler — регистрация одного консьюмера.
//
// amqp.DefaultMarshaler заменяется на marshaler.DeathTolerantMarshaler с теми же настройками:
// повторы SDK возвращаются через DLX с заголовком x-death, который DefaultMarshaler не разбирает.
// NoRequeueOnNack SDK не включает: при остановке watermill отклоняет недоставленные в обработчик
// сообщения через Nack, и без requeue здоровые сообщения ушли бы в DLX сервиса.
func (a *Consumer) RegisterHandler(ctx context.Context, handler config.Handler, cfg ...config.Config) error {
	cc := &amqp.Config{}
	for _, opt := range cfg {
		opt(cc)
	}

	cc.Marshaler = deathTolerant(cc.Marshaler)

	ownDLX := hasDeadLetterExchange(cc.Queue.Arguments)

	consumer, err := amqp.NewSubscriberWithConnection(*cc, a.logger, a.conn)
	if err != nil {
		logger.Error(ctx, "failed to subscribe to queue: "+err.Error())

		return err
	}

	entry := &consumerEntry{
		ConsumeConfig: config.ConsumeConfig{Consumer: consumer, Handler: handler},
		queue:         generate(cc.Queue.GenerateName, ""),
		exchange:      generate(cc.Exchange.GenerateName, ""),
		routingKey:    generate(cc.QueueBind.GenerateRoutingKey, ""),
		ownDLX:        ownDLX,
		marshaler:     cc.Marshaler,
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.closed {
		_ = consumer.Close()

		return errors.New("consumer closed")
	}

	a.handlers = append(a.handlers, entry)

	return nil
}

// RunConsumers — удобный метод: регистрирует и запускает Consume.
// ВАЖНО: передавайте ctx, чтобы можно было остановить обработку и корректно закрыть соединение.
func (a *Consumer) RunConsumers(ctx context.Context, handlers []config.HandlerRegister) error {
	if err := a.RegisterMultipleHandler(ctx, handlers); err != nil {
		return err
	}

	return a.Consume(ctx)
}

// Consume — запускает все зарегистрированные handlers и блокируется до остановки.
//
// Отмена ctx (или Close) - нормальная остановка: приём прекращается, текущие обработчики
// дорабатывают (их ctx не отменяется), Consume возвращает nil после их завершения.
// Ошибка подписки, паника вне обработчика или закрытие канала сообщений без остановки - отмена
// всех подписчиков и ошибка: консьюмер не остаётся «живым» без приёма сообщений.
// Обработка сообщения: handler ok => Ack; handler err/panic => по политике (по умолчанию ActionRetry).
func (a *Consumer) Consume(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil context")
	}

	if a.conn == nil {
		return errors.New("nil rabbit connection")
	}

	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()

		return errors.New("consumer closed")
	}

	handlers := append([]*consumerEntry(nil), a.handlers...)
	if len(handlers) == 0 {
		a.mu.Unlock()

		return errors.New("no handlers registered")
	}

	// общий контекст: если один консьюмер упал - останавливаем всех
	ctx, cancel := context.WithCancel(ctx)
	a.cancels = append(a.cancels, cancel)
	a.running.Add(1)
	a.mu.Unlock()

	defer a.running.Done()
	defer cancel()

	// ctx обработчика не отменяется остановкой: начатая обработка дорабатывает до конца
	handlerCtx := context.WithoutCancel(ctx)

	errCh := make(chan error, len(handlers))
	var wg sync.WaitGroup

	fail := func(err error) {
		select {
		case errCh <- err:
		default:
		}
		cancel()
	}

	for _, entry := range handlers {
		wg.Add(1)

		go func(e *consumerEntry) {
			defer wg.Done()

			// Защита от паники на уровне консьюмера (вне обработки конкретного msg)
			defer func() {
				if r := recover(); r != nil {
					err := fmt.Errorf("consumer goroutine panic: %v", r)
					a.captureSentry(ctx, err, e, nil, true)
					fail(err)
				}
			}()

			handlerName := getHandlerName(e.Handler)
			logger.Info(ctx, "Try to subscribe handler= "+handlerName)

			msgChannel, err := e.Consumer.Subscribe(ctx, "")
			if err != nil {
				wrapped := fmt.Errorf("failed to subscribe (handler=%s): %w", handlerName, err)
				a.captureSentry(ctx, wrapped, e, nil, false)
				fail(wrapped)

				return
			}

			logger.Info(ctx, "Successfully subscribed handler="+handlerName)

			for {
				select {
				case <-ctx.Done():
					logger.Info(ctx, "Context cancelled, stopping consumer handler="+handlerName)

					return

				case msg, ok := <-msgChannel:
					if !ok {
						if ctx.Err() != nil {
							logger.Info(ctx, "Message channel closed handler="+handlerName)

							return
						}

						fail(fmt.Errorf("message channel closed unexpectedly (handler=%s)", handlerName))

						return
					}

					a.handleMessage(handlerCtx, e, msg)
				}
			}
		}(entry)
	}

	// закрываем errCh, когда все горутины завершились
	go func() {
		wg.Wait()
		close(errCh)
	}()

	// ждём завершения всех горутин (они дорабатывают текущее сообщение) и возвращаем первую ошибку
	var firstErr error
	for err := range errCh {
		if firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// Close — остановка: прекращает приём, ждёт текущие обработчики в пределах ctx, затем закрывает
// подписчиков. Соединение ядра не закрывает. Если ctx истёк раньше - подписчики закрываются сразу
// (необработанные сообщения брокер вернёт в очередь), возвращается ошибка ctx. Повторный вызов - nil.
func (a *Consumer) Close(ctx context.Context) error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()

		return nil
	}

	a.closed = true
	cancels := a.cancels
	handlers := a.handlers
	a.mu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}

	done := make(chan struct{})
	go func() {
		a.running.Wait()
		close(done)
	}()

	var waitErr error
	select {
	case <-done:
	case <-ctx.Done():
		waitErr = fmt.Errorf("rabbit consumer: handlers still running: %w", ctx.Err())
		logger.Error(ctx, waitErr.Error())
	}

	var closeErr error
	for _, e := range handlers {
		if err := e.Consumer.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}

	a.mu.Lock()
	retry := a.retry
	a.retryClosed = true
	a.mu.Unlock()

	if retry != nil {
		retry.close()
	}

	return errors.Join(waitErr, closeErr)
}

// handleMessage ctx обработчика несёт id корреляции из метаданных correlation_id сообщения (нет — новый id).
func (a *Consumer) handleMessage(ctx context.Context, e *consumerEntry, msg *message.Message) {
	handlerName := getHandlerName(e.Handler)
	ctx = consumeCorrelation(ctx, msg)

	// Защита от паники внутри конкретного сообщения
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic in handler=%s: %v", handlerName, r)
			log.Printf("Panic: %v", err)

			a.captureSentry(ctx, err, e, msg, true)
			a.applyAction(ctx, e, msg, a.onPanic, err)
		}
	}()

	start := time.Now()
	err := e.Handler(ctx, msg)
	elapsed := time.Since(start)

	if err != nil {
		log.Printf("Error handling message handler=%s elapsed=%s err=%v", handlerName, elapsed, err)
		a.captureSentry(ctx, err, e, msg, false)
		a.applyAction(ctx, e, msg, a.onError, err)

		return
	}

	// успех
	msg.Ack()
}

func (a *Consumer) applyAction(ctx context.Context, e *consumerEntry, msg *message.Message, action ErrorAction, cause error) {
	switch action {
	case ActionAck:
		msg.Ack()
	case ActionNack:
		msg.Nack()
	default:
		a.retryOrPark(ctx, e, msg, cause)
	}
}

// retryOrPark Политика ActionRetry: повтор с паузой, после предела - .dlq. Исходное сообщение
// подтверждается только после подтверждения брокером копии; иначе - Nack.
// У очереди со своим DLX - сразу Nack: повторы задаёт политика DLX сервиса.
func (a *Consumer) retryOrPark(ctx context.Context, e *consumerEntry, msg *message.Message, cause error) {
	attempt := attemptOf(msg)
	decision := decideFailure(attempt, e.queue, e.ownDLX, retryDelays)

	switch decision.kind {
	case failureServiceDLX:
		logger.Error(ctx, fmt.Sprintf("rabbit consumer: queue=%s message=%s: handler failed, nacked to the queue's own dead-letter policy",
			e.queue, msg.UUID))
		msg.Nack()

		return
	case failureNack:
		msg.Nack()

		return
	}

	pub := a.retryPublisher()
	if pub == nil {
		msg.Nack()

		return
	}

	pubCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), republishTimeout)
	defer cancel()

	var err error
	if decision.kind == failureRetry {
		err = a.publishRetry(pubCtx, pub, e, msg, attempt+1, decision.delay)
	} else {
		err = a.publishParked(pubCtx, pub, e, msg, attempt, cause)
	}

	if err != nil {
		logger.Error(ctx, fmt.Sprintf("rabbit consumer: queue=%s message=%s: %v; message nacked", e.queue, msg.UUID, err))
		msg.Nack()

		return
	}

	if decision.kind == failurePark {
		logger.Error(ctx, fmt.Sprintf("rabbit consumer: queue=%s message=%s: retries exhausted (%d), parked in %s",
			e.queue, msg.UUID, attempt, parkingQueueName(e.queue)))
	}

	msg.Ack()
}

func (a *Consumer) publishRetry(ctx context.Context, pub *confirmPublisher, e *consumerEntry, msg *message.Message, attempt int, delay time.Duration) error {
	queue := retryQueueName(e.queue, delay)
	target := e.queue

	publishing, err := republishing(e.marshaler, msg, map[string]string{AttemptHeader: strconv.Itoa(attempt)})
	if err != nil {
		return err
	}

	declare := func(ch *stdamqp.Channel) error {
		_, err := ch.QueueDeclare(queue, true, false, false, false, delayQueueArgs(delay, "", &target))

		return err
	}

	if err := pub.publishDeclared(ctx, queue, declare, "", queue, publishing); err != nil {
		return fmt.Errorf("retry %d via %s: %w", attempt, queue, err)
	}

	return nil
}

func (a *Consumer) publishParked(ctx context.Context, pub *confirmPublisher, e *consumerEntry, msg *message.Message, attempt int, cause error) error {
	queue := parkingQueueName(e.queue)

	headers := map[string]string{
		AttemptHeader:       strconv.Itoa(attempt),
		OriginalQueueHeader: e.queue,
	}
	if cause != nil {
		headers[LastErrorHeader] = truncate(cause.Error(), lastErrorMaxLen)
	}

	publishing, err := republishing(e.marshaler, msg, headers)
	if err != nil {
		return err
	}

	// Уже существующая очередь (в том числе объявленная сервисом со своими аргументами) берётся как есть.
	declare := func(ch *stdamqp.Channel) error {
		return declareParkingQueue(pub, queue, ch)
	}

	if err := pub.publishDeclared(ctx, queue, declare, "", queue, publishing); err != nil {
		return fmt.Errorf("park in %s: %w", queue, err)
	}

	return nil
}

// declareParkingQueue Пассивная проверка, иначе - объявление durable-очереди без аргументов.
// Пассивная проверка отсутствующей очереди закрывает канал - объявление идёт на новом.
func declareParkingQueue(pub *confirmPublisher, queue string, ch *stdamqp.Channel) error {
	if _, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil); err == nil {
		return nil
	}

	return pub.withTempChannel(func(ch *stdamqp.Channel) error {
		_, err := ch.QueueDeclare(queue, true, false, false, false, nil)

		return err
	})
}

// republishing Копия сообщения для повтора или парковки: тело и метаданные исходного, persistent.
func republishing(m amqp.Marshaler, msg *message.Message, headers map[string]string) (stdamqp.Publishing, error) {
	if m == nil {
		m = amqp.DefaultMarshaler{}
	}

	out := msg.Copy()
	delete(out.Metadata, deathCountMetadataKey)

	for k, v := range headers {
		out.Metadata.Set(k, v)
	}

	publishing, err := m.Marshal(out)
	if err != nil {
		return stdamqp.Publishing{}, fmt.Errorf("marshal: %w", err)
	}

	publishing.DeliveryMode = stdamqp.Persistent

	return publishing, nil
}

// retryPublisher Публикатор повторов (создаётся при первом обращении); после Close - nil.
func (a *Consumer) retryPublisher() *confirmPublisher {
	if a.conn == nil {
		return nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.retry == nil && !a.retryClosed {
		a.retry = newConfirmPublisher(a.conn)
	}

	return a.retry
}

// captureSentry отправляет ошибку в error-трекер через errorreporter.Capture.
// Consumer ничего не знает про Sentry - реальную отправку делает адаптер,
// зарегистрированный через errorreporter.SetReporter (см. gosdk-sentry-core).
// Без него вызов безопасен и просто ничего не отправляет.
//
// Отправляются только безопасные поля: id и размер сообщения, очередь, exchange, ключ привязки,
// номер повтора. Payload - только если сервис явно задал WithSentryPayloadLimit(n > 0);
// произвольные заголовки (metadata) не отправляются.
func (a *Consumer) captureSentry(ctx context.Context, err error, e *consumerEntry, msg *message.Message, isPanic bool) {
	handlerName := getHandlerName(e.Handler)

	consumerCtx := map[string]any{
		"handler": handlerName,
		"panic":   isPanic,
	}

	if e.queue != "" {
		consumerCtx["queue"] = e.queue
	}

	if e.exchange != "" {
		consumerCtx["exchange"] = e.exchange
	}

	if e.routingKey != "" {
		consumerCtx["routing_key"] = e.routingKey
	}

	if msg != nil {
		consumerCtx["message_uuid"] = msg.UUID
		consumerCtx["payload_size"] = len(msg.Payload)
		consumerCtx["attempt"] = attemptOf(msg)

		if a.sentryPayload > 0 {
			payload := msg.Payload
			truncated := len(payload) > a.sentryPayload
			if truncated {
				payload = payload[:a.sentryPayload]
			}

			consumerCtx["payload"] = string(payload)
			consumerCtx["payload_truncated"] = truncated
		}
	}

	// Level: error для всех случаев - как и раньше (в исходном коде
	// scope.SetLevel не звался, sentry-go по умолчанию шлёт LevelError).
	errorreporter.Capture(ctx, err, errorreporter.Options{
		Level: errorreporter.LevelError,
		// handler тегом (не только в Extra) - чтобы фильтровать/группировать
		// issues в Sentry по конкретному консьюмеру, как endpoint для HTTP.
		Tags: map[string]string{
			"component": "rabbit_consumer",
			"handler":   handlerName,
		},
		Extra: map[string]any{"consumer": consumerCtx},
	})
}

func getHandlerName(handler config.Handler) string {
	handlerName := reflect.TypeOf(handler).String()

	if reflect.TypeOf(handler).Kind() == reflect.Func {
		pc := reflect.ValueOf(handler).Pointer()
		if fn := runtime.FuncForPC(pc); fn != nil {
			handlerName = fn.Name()
		}
	}

	return handlerName
}

// deathTolerant amqp.DefaultMarshaler (значение или указатель) - в DeathTolerantMarshaler с теми же
// настройками; свой marshaler сервиса - без изменений.
func deathTolerant(m amqp.Marshaler) amqp.Marshaler {
	switch v := m.(type) {
	case amqp.DefaultMarshaler:
		return marshaler.DeathTolerantMarshaler{DefaultMarshaler: v}
	case *amqp.DefaultMarshaler:
		if v != nil {
			return marshaler.DeathTolerantMarshaler{DefaultMarshaler: *v}
		}
	}

	return m
}

// hasDeadLetterExchange Задан ли у очереди x-dead-letter-exchange в аргументах конфига.
func hasDeadLetterExchange(args stdamqp.Table) bool {
	_, ok := args["x-dead-letter-exchange"]

	return ok
}

func generate(fn func(string) string, topic string) string {
	if fn == nil {
		return ""
	}

	return fn(topic)
}
