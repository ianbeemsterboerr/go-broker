package broker

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/streadway/amqp"
)

type rabbitConnectionBuilder struct {
	address        string
	user           string
	password       string
	allowedRetries int
}

func NewConnectionBuilder() *rabbitConnectionBuilder {
	return &rabbitConnectionBuilder{allowedRetries: 10}
}

func (v *rabbitConnectionBuilder) WithAddress(address string) *rabbitConnectionBuilder {
	v.address = address
	return v
}

func (v *rabbitConnectionBuilder) WithUser(user string) *rabbitConnectionBuilder {
	v.user = user
	return v
}

func (v *rabbitConnectionBuilder) WithPassword(password string) *rabbitConnectionBuilder {
	v.password = password
	return v
}

func (v *rabbitConnectionBuilder) WithMaximumRetries(allowedRetries int) *rabbitConnectionBuilder {
	v.allowedRetries = allowedRetries
	return v
}

// Uses MESSAGEBUS_ADDRESS, MESSAGEBUS_USER, MESSAGEBUS_PASS loaded from environment variables to connect to RabbitMQ Message Broker.
func (v *rabbitConnectionBuilder) WithConfigLoadedFromEnvVariables() *rabbitConnectionBuilder {
	v.address = os.Getenv("MESSAGEBUS_ADDRESS")
	v.user = os.Getenv("MESSAGEBUS_USER")
	v.password = os.Getenv("MESSAGEBUS_PASS")
	return v
}

func (v *rabbitConnectionBuilder) Connect() (*amqp.Connection, error) {
	conn := &amqp.Connection{}
	err := errors.New("")
	tries := 0
	for tries < v.allowedRetries && err != nil {
		conn, err = amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s", v.user, v.password, v.address))
		if err != nil {
			fmt.Println(err)
			time.Sleep(time.Second * 1)
			tries++
		}
	}
	return conn, err
}
