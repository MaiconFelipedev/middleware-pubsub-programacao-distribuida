package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"

	"pubsub/internal/client"
)

func main() {
	c, err := client.Connect("localhost:9000")
	if err != nil {
		log.Fatal("erro ao conectar:", err)
	}
	defer c.Close()

	// Inscreve nos tópicos passados por argumento
	for _, topic := range os.Args[1:] {
		if err := c.Subscribe(topic); err != nil {
			log.Fatal("erro ao se inscrever:", err)
		}

		resp, err := c.Read()
		if err != nil {
			log.Fatal("erro ao ler resposta:", err)
		}

		fmt.Println("RECEBIDO:", resp)
	}

	// Goroutine só para ler mensagens do broker
	go func() {
		for {
			resp, err := c.Read()
			if err != nil {
				log.Fatal("erro ao ler mensagem:", err)
			}
			fmt.Println("RECEBIDO:", resp)
		}
	}()

	fmt.Println("subscriber pronto")
	fmt.Println("comandos:")
	fmt.Println("  sub <topic>")
	fmt.Println("  unsub <topic>")
	fmt.Println("  quit")

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)

		if line == "" {
			continue
		}

		if line == "quit" {
			fmt.Println("encerrando subscriber...")
			return
		}

		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			fmt.Println("comando inválido")
			continue
		}

		cmd := parts[0]
		topic := strings.TrimSpace(parts[1])

		switch cmd {
		case "sub":
			if err := c.Subscribe(topic); err != nil {
				fmt.Println("erro ao se inscrever:", err)
			}

		case "unsub":
			if err := c.Unsubscribe(topic); err != nil {
				fmt.Println("erro ao remover inscrição:", err)
			}

		default:
			fmt.Println("comando inválido")
		}
	}
}