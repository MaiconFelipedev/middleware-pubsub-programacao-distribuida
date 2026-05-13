package main

import (
	"log"
	"net"

	"pubsub/internal/broker"
)

func main() {
	addr := ":9000"

	b := broker.New()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("erro ao iniciar broker: %v", err)
	}
	defer ln.Close()

	log.Printf("broker escutando em %s", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("erro ao aceitar conexão: %v", err)
			continue
		}

		go b.HandleConnection(conn)
	}
}