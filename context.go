package broker

import (
	"fmt"
	"log"

	guuid "github.com/google/uuid"
	"github.com/streadway/amqp"
)

// Context is the context which holds all the config and connections to the service bus. Before starting the Listen() function, command- and eventlisteners should be added to this context.
type Context struct {
	conn             *amqp.Connection
	exchangeName     string
	eventListeners   map[string]func(body []byte)
	commandListeners map[string]func(body []byte) []byte
	sendingChannel   *amqp.Channel
}

// NewBrokerContext Accepts a pointer to a working amqp.Connection and an exchange name, returns a pointer to a BrokerContext.
func NewBrokerContext(conn *amqp.Connection, exchangeName string) *Context {
	l := make(map[string]func(body []byte))
	cl := make(map[string]func(body []byte) []byte)
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("%s", err)
	}
	return &Context{conn: conn, exchangeName: exchangeName, eventListeners: l, sendingChannel: ch, commandListeners: cl}
}

// AddEventListener accepts a queueName, and starts listening on that queue. When a message is received, the onMessageReceive function is called.
func (broker *Context) AddEventListener(queueName string, onMessageReceive func(body []byte)) {
	broker.eventListeners[queueName] = onMessageReceive
}

// AddCommandListener handles RPC messages
func (broker *Context) AddCommandListener(queueName string, onMessageReceive func(body []byte) []byte) {
	broker.commandListeners[queueName] = onMessageReceive
}

// Listen starts up all the MessageListeners
func (broker *Context) Listen() {
	forever := make(chan bool)
	for queueName, listener := range broker.eventListeners {
		go broker.activateEventListener(queueName, listener)
	}
	for queueName, handler := range broker.commandListeners {
		go broker.activateCommandListener(queueName, handler)
	}
	<-forever
}

func (broker *Context) activateCommandListener(queueName string, handleCommand func(body []byte) []byte) {
	ch, err := broker.conn.Channel()
	failOnError(err, "Failed to open a channel: ")
	defer ch.Close()
	err = declareExchange(broker.exchangeName, ch)
	failOnError(err, "Failed to declare an exchange: ")

	q, err := declareQueue(queueName, ch)
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	err = ch.QueueBind(q.Name, q.Name, broker.exchangeName, false, nil)
	if err != nil {
		log.Printf("%s", err)
	}
	failOnError(err, "Failed to declare a queue: ")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	log.Printf("Started listening for commands on queue %s on exchange %s", queueName, broker.exchangeName)

	forever := make(chan bool)
	go func() {
		for d := range msgs {

			res := handleCommand(d.Body)
			err = ch.Publish(
				broker.exchangeName, // exchange
				d.ReplyTo,           // routing key
				false,               // mandatory
				false,               // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          res,
				})
			failOnError(err, "Failed to publish a message")

			d.Ack(false)
		}
	}()
	<-forever
}

// EmitCommand Emits a command over the eventbus. Expects a returnvalue
func (broker *Context) EmitCommand(routingKey string, body []byte) (res []byte, err error) {
	replyQueueName := fmt.Sprintf("reply.%s", routingKey)
	ch, err := broker.conn.Channel()
	q, err := ch.QueueDeclare(
		replyQueueName, // name
		false,          // durable
		false,          // delete when unused
		true,           // exclusive
		false,          // noWait
		nil,            // arguments
	)

	ch.QueueBind(replyQueueName, replyQueueName, broker.exchangeName, false, nil)

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	correlationID := guuid.New()
	err = ch.Publish(
		broker.exchangeName, // exchange
		routingKey,          // routing key of queue handling rpc commands
		false,               // mandatory
		false,               // immediate
		amqp.Publishing{
			ReplyTo:       replyQueueName,
			CorrelationId: correlationID.String(),
			ContentType:   "text/plain",
			Body:          []byte(body),
		})

	failOnError(err, "Failed to publish a message")
	for d := range msgs {
		if correlationID.String() == d.CorrelationId {
			res = d.Body
			break
		}
	}

	return
}

func (broker *Context) activateEventListener(queueName string, onMessageReceive func(body []byte)) {
	ch, err := broker.conn.Channel()
	failOnError(err, "Failed to open a channel: ")
	defer ch.Close()
	err = declareExchange(broker.exchangeName, ch)
	failOnError(err, "Failed to declare an exchange: ")

	q, err := declareQueue(queueName, ch)
	err = ch.QueueBind(q.Name, q.Name, broker.exchangeName, false, nil)
	if err != nil {
		log.Printf("%s", err)
	}
	failOnError(err, "Failed to declare a queue: ")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	log.Printf("Started listening on queue %s on exchange %s", queueName, broker.exchangeName)

	forever := make(chan bool)
	go func() {
		for d := range msgs {
			onMessageReceive(d.Body)
		}
	}()
	<-forever
}

//EmitEvent sends a message to specified routingkey. It gets the exchange from the broker itself.
func (broker *Context) EmitEvent(routingKey string, body []byte) {
	err := broker.sendingChannel.Publish(
		broker.exchangeName, // exchange
		routingKey,          // routing key
		false,               // mandatory
		false,               // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func declareExchange(exchangeName string, ch *amqp.Channel) error {
	err := ch.ExchangeDeclare(
		exchangeName, // name
		"topic",      // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	return err
}

func declareQueue(queueName string, ch *amqp.Channel) (amqp.Queue, error) {
	q, err := ch.QueueDeclare(
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	return q, err
}
