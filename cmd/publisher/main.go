package main

import (
	"fmt"
	"log"
	"os"

	"pubsub/internal/client"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println(`uso: go run ./cmd/publisher <topic> <json>
exemplo: go run ./cmd/publisher chat "{\"user\":\"ana\",\"msg\":\"oi\"}"`)
		return
	}

	topic := os.Args[1]
	data := os.Args[2]

	c, err := client.Connect("localhost:9000")
	if err != nil {
		log.Fatal("erro ao conectar:", err)
	}
	defer c.Close()

	if err := c.Publish(topic, data); err != nil {
		log.Fatal("erro ao publicar:", err)
	}

	fmt.Println("mensagem enviada, aguardando resposta do broker...")

	resp, err := c.Read()
	if err != nil {
		log.Fatal("erro ao ler resposta:", err)
	}

	fmt.Println("RECEBIDO:", resp)
}