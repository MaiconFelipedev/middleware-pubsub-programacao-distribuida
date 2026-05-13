package client

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"pubsub/internal/protocol"
)

type Client struct {
	conn    net.Conn
	encoder *json.Encoder
	scanner *bufio.Scanner
	mu      sync.Mutex
}

func Connect(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:    conn,
		encoder: json.NewEncoder(conn),
		scanner: bufio.NewScanner(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) send(req protocol.Request) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.encoder.Encode(req)
}

func (c *Client) Subscribe(topic string) error {
	return c.send(protocol.Request{
		Action: "subscribe",
		Topic:  topic,
	})
}

func (c *Client) Unsubscribe(topic string) error {
	return c.send(protocol.Request{
		Action: "unsubscribe",
		Topic:  topic,
	})
}

func (c *Client) Publish(topic, data string) error {
	return c.send(protocol.Request{
		Action: "publish",
		Topic:  topic,
		Data:   data,
	})
}

func (c *Client) Read() (protocol.Response, error) {
	if !c.scanner.Scan() {
		if err := c.scanner.Err(); err != nil {
			return protocol.Response{}, err
		}
		return protocol.Response{}, fmt.Errorf("conexão encerrada")
	}

	var resp protocol.Response
	if err := json.Unmarshal(c.scanner.Bytes(), &resp); err != nil {
		return protocol.Response{}, err
	}

	return resp, nil
}