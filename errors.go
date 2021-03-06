package gorabbit

import amqp "github.com/rabbitmq/amqp091-go"

var (
	// ErrClosed If closed channel or connection to rabbitmq
	ErrClosed = amqp.ErrClosed

	// ErrChannelMax is returned when Connection.Channel has been called enough
	// times that all channel IDs have been exhausted in the client or the
	// server.
	ErrChannelMax = amqp.ErrChannelMax

	// ErrSASL is returned from Dial when the authentication mechanism could not
	// be negotiated.
	ErrSASL = amqp.ErrSASL

	// ErrCredentials is returned when the authenticated client is not authorized
	// to any vhost.
	ErrCredentials = amqp.ErrCredentials

	// ErrVhost is returned when the authenticated user is not permitted to
	// access the requested Vhost.
	ErrVhost = amqp.ErrVhost

	// ErrSyntax is hard protocol error, indicating an unsupported protocol,
	// implementation or encoding.
	ErrSyntax = amqp.ErrSyntax

	// ErrFrame is returned when the protocol frame cannot be read from the
	// server, indicating an unsupported protocol or unsupported frame type.
	ErrFrame = amqp.ErrFrame

	// ErrCommandInvalid is returned when the server sends an unexpected response
	// to this requested message type. This indicates a bug in this client.
	ErrCommandInvalid = amqp.ErrCommandInvalid

	// ErrUnexpectedFrame is returned when something other than a method or
	// heartbeat frame is delivered to the Connection, indicating a bug in the
	// client.
	ErrUnexpectedFrame = amqp.ErrUnexpectedFrame

	// ErrFieldType is returned when writing a message containing a Go type unsupported by AMQP.
	ErrFieldType = amqp.ErrFieldType
)
