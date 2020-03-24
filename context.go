package broker

import (
	"log"

	"github.com/streadway/amqp"
)

type Receiver interface {
	Receive(routingKey string, onMessageReceive func(body []byte))
}

type Sender interface {
	Send(routingKey string, body []byte)
}

type BrokerContext struct {
	conn         *amqp.Connection
	receivers    []*Receiver
	senders      []*Sender
	exchangeName string
}

// NewBrokerContext Accepts a pointer to a working amqp.Connection, returns a pointer to a BrokerContext.
func NewBrokerContext(conn *amqp.Connection, exchangeName string) *BrokerContext {
	return &BrokerContext{conn: conn, receivers: []*Receiver{}, senders: []*Sender{}, exchangeName: exchangeName}
}

// AddMessageListener accepts a queueName, and starts listening on that queue. When a message is received, the onMessageReceive function is called.
func (broker *BrokerContext) AddMessageListener(queueName string, onMessageReceive func(body []byte)) {
	ch, err := broker.conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = declareExchange(broker.exchangeName, ch)
	failOnError(err, "Failed to declare an exchange")

	q, err := declareQueue(queueName, ch)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	go func() {
		for d := range msgs {
			onMessageReceive(d.Body)
		}
	}()
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}

func declareExchange(exchangeName string, ch *amqp.Channel) error {
	err := ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type
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
