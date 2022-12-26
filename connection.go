package amqptransport

import (
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type connectionManager struct {
	logger Logger

	connection *amqp.Connection
	channel    *amqp.Channel

	uri               string
	reconnectInterval time.Duration

	channelMutex   *sync.RWMutex
	channelRestart chan error

	flowPause bool
	flowMutex *sync.RWMutex

	blockedConn      bool
	blockedConnMutex *sync.RWMutex
}

func (c *connectionManager) getChannel() *amqp.Channel {
	c.channelMutex.RLock()
	defer c.channelMutex.RUnlock()

	return c.channel
}

func (c *connectionManager) onChannelRestart() chan error {
	return c.channelRestart
}

func (c *connectionManager) notifyChannelRestart() {
	notifyClose := c.channel.NotifyClose(make(chan *amqp.Error))
	notifyCancel := c.channel.NotifyCancel(make(chan string))

	select {
	case err := <-notifyClose:
		if err != nil {
			c.logger.Error("channel closed with error: %v, attempting to reconnect", err)
			c.reconnect()
			c.logger.Info("channel successfully reconnected after close")
			c.channelRestart <- err
		} else {
			c.logger.Info("channel closed gracefully")
		}
	case cancel := <-notifyCancel:
		c.logger.Error("channel cancelled with error: %v, attempting to reconnect", cancel)
		c.reconnect()
		c.logger.Info("channel successfully reconnected after cancel")
		c.channelRestart <- nil
	}
}

func (c *connectionManager) notifyFlow() {
	notifyFlow := c.channel.NotifyFlow(make(chan bool))

	c.flowMutex.Lock()
	c.flowPause = false
	c.flowMutex.Unlock()

	for ok := range notifyFlow {
		c.flowMutex.Lock()
		if ok {
			c.logger.Info("pausing publishing due to flow control request from server")
			c.flowPause = true
		} else {
			c.logger.Info("resuming publishing after flow control request from server")
			c.flowPause = false
		}
		c.flowMutex.Unlock()
	}
}

func (c *connectionManager) checkFlow() error {
	c.flowMutex.RLock()
	defer c.flowMutex.RUnlock()

	if c.flowPause {
		return errors.New("publishing paused due to flow control request from server")
	}

	return nil
}

func (c *connectionManager) notifyBlocked() {
	notifyBlocked := c.connection.NotifyBlocked(make(chan amqp.Blocking))

	for blocked := range notifyBlocked {
		c.blockedConnMutex.Lock()
		if blocked.Active {
			c.logger.Error("connection blocked with reason: %v", blocked.Reason)
			c.blockedConn = true
		} else {
			c.logger.Info("connection unblocked")
			c.blockedConn = false
		}
		c.blockedConnMutex.Unlock()
	}
}

func (c *connectionManager) checkBlocked() error {
	c.blockedConnMutex.RLock()
	defer c.blockedConnMutex.RUnlock()

	if c.blockedConn {
		return errors.New("connection blocked")
	}

	return nil
}

func (c *connectionManager) reconnect() {
	c.channelMutex.Lock()
	defer c.channelMutex.Unlock()

	for {
		c.logger.Info("waiting for %s seconds before attempting to reconnect to server at: %s", c.reconnectInterval, c.uri)
		time.Sleep(c.reconnectInterval)

		ch, conn, err := getNewChannel(c.uri)
		if err != nil {
			c.logger.Error("failed to reconnect to server at: %s, error: %v", c.uri, err)
			continue
		} else {
			c.channel.Close()
			c.connection.Close()
			c.connection = conn
			c.channel = ch

			go c.notifyChannelRestart()
			go c.notifyFlow()
			go c.notifyBlocked()

			c.logger.Info("successfully reconnected to server at: %s", c.uri)

			break
		}
	}
}

func (c *connectionManager) close() error {
	c.channelMutex.Lock()
	defer c.channelMutex.Unlock()

	err := c.channel.Close()
	if err != nil {
		return err
	}

	err = c.connection.Close()
	if err != nil {
		return err
	}

	return nil
}

func getNewChannel(url string) (*amqp.Channel, *amqp.Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	return ch, conn, nil
}

func newConnectionMananger(uri string, reconnectInterval time.Duration, logger Logger) (*connectionManager, error) {
	ch, conn, err := getNewChannel(uri)
	if err != nil {
		return nil, err
	}

	c := &connectionManager{
		logger:            logger,
		connection:        conn,
		channel:           ch,
		uri:               uri,
		reconnectInterval: reconnectInterval,
		channelMutex:      &sync.RWMutex{},
		channelRestart:    make(chan error),
		flowMutex:         &sync.RWMutex{},
		blockedConnMutex:  &sync.RWMutex{},
	}

	go c.notifyChannelRestart()
	go c.notifyFlow()
	go c.notifyBlocked()

	return c, nil
}
