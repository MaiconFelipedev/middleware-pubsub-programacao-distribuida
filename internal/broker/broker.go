package broker

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"sync"

	"pubsub/internal/protocol"
)

type Client struct {
	Conn   net.Conn
	Send   chan protocol.Response
	Topics map[string]bool
}

type Broker struct {
	mu     sync.RWMutex
	topics map[string]map[*Client]bool
	queues map[string]chan protocol.Response
}

func New() *Broker {
	return &Broker{
		topics: make(map[string]map[*Client]bool),
		queues: make(map[string]chan protocol.Response),
	}
}

func (b *Broker) HandleConnection(conn net.Conn) {
	client := &Client{
		Conn:   conn,
		Send:   make(chan protocol.Response, 32),
		Topics: make(map[string]bool),
	}

	log.Printf("cliente conectado: %s", conn.RemoteAddr())

	go b.writeLoop(client)
	defer b.cleanupClient(client)

	dec := json.NewDecoder(bufio.NewReader(conn))

	for {
		var req protocol.Request
		if err := dec.Decode(&req); err != nil {
			if err != io.EOF {
				log.Printf("erro ao decodificar: %v", err)
			}
			return
		}

		switch req.Action {
		case "subscribe":
			b.subscribe(client, req.Topic)

		case "unsubscribe":
			b.unsubscribe(client, req.Topic)

		case "publish":
			b.publish(client, req.Topic, req.Data)

		default:
			client.Send <- protocol.Response{
				Type:   "ack",
				Status: "error",
				Error:  "unknown action",
			}
		}
	}
}

func (b *Broker) writeLoop(client *Client) {
	enc := json.NewEncoder(client.Conn)

	for resp := range client.Send {
		if err := enc.Encode(resp); err != nil {
			log.Printf("erro ao enviar resposta: %v", err)
			return
		}
	}
}

func (b *Broker) subscribe(client *Client, topic string) {
	if topic == "" {
		client.Send <- protocol.Response{
			Type:   "ack",
			Status: "error",
			Error:  "topic is required",
		}
		return
	}

	b.mu.Lock()

	if _, exists := b.topics[topic]; !exists {
		b.topics[topic] = make(map[*Client]bool)
		queue := make(chan protocol.Response, 128)
		b.queues[topic] = queue
		go b.dispatchTopic(topic, queue)
	}

	b.topics[topic][client] = true
	client.Topics[topic] = true

	b.mu.Unlock()

	client.Send <- protocol.Response{
		Type:   "ack",
		Status: "subscribed",
		Topic:  topic,
	}
}

func (b *Broker) unsubscribe(client *Client, topic string) {
	if topic == "" {
		client.Send <- protocol.Response{
			Type:   "ack",
			Status: "error",
			Error:  "topic is required",
		}
		return
	}

	b.mu.Lock()

	subs, exists := b.topics[topic]
	if exists {
		delete(subs, client)
		delete(client.Topics, topic)

		if len(subs) == 0 {
			close(b.queues[topic])
			delete(b.queues, topic)
			delete(b.topics, topic)
		}
	}

	b.mu.Unlock()

	client.Send <- protocol.Response{
		Type:   "ack",
		Status: "unsubscribed",
		Topic:  topic,
	}
}

func (b *Broker) publish(client *Client, topic, data string) {
	if topic == "" {
		client.Send <- protocol.Response{
			Type:   "ack",
			Status: "error",
			Error:  "topic is required",
		}
		return
	}

	b.mu.RLock()
	queue, topicExists := b.queues[topic]
	subscribers := len(b.topics[topic])
	b.mu.RUnlock()

	if !topicExists || subscribers == 0 {
		client.Send <- protocol.Response{
			Type:   "ack",
			Status: "discarded",
			Topic:  topic,
			Error:  "no subscribers",
		}
		return
	}

	// Bufferiza a mensagem: recebimento e encaminhamento ficam desacoplados
	queue <- protocol.Response{
		Type:  "deliver",
		Topic: topic,
		Data:  data,
	}

	client.Send <- protocol.Response{
		Type:   "ack",
		Status: "published",
		Topic:  topic,
	}
}

func (b *Broker) dispatchTopic(topic string, queue <-chan protocol.Response) {
	for msg := range queue {
		b.mu.RLock()

		subsMap, exists := b.topics[topic]
		if !exists {
			b.mu.RUnlock()
			continue
		}

		subscribers := make([]*Client, 0, len(subsMap))
		for client := range subsMap {
			subscribers = append(subscribers, client)
		}

		b.mu.RUnlock()

		for _, client := range subscribers {
			select {
			case client.Send <- msg:
			default:
				log.Printf("cliente lento; descartando entrega do tópico %s", topic)
			}
		}
	}
}

func (b *Broker) cleanupClient(client *Client) {
	b.mu.Lock()

	for topic := range client.Topics {
		if subs, exists := b.topics[topic]; exists {
			delete(subs, client)

			if len(subs) == 0 {
				close(b.queues[topic])
				delete(b.queues, topic)
				delete(b.topics, topic)
			}
		}
	}

	b.mu.Unlock()

	close(client.Send)
	_ = client.Conn.Close()

	log.Printf("cliente desconectado")
}