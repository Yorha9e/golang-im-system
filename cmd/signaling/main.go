package main

import (
	"flag"
	"log"

	"golang-im-system/signaling"
)

func main() {
	addr := flag.String("addr", ":7000", "signaling server listen address")
	flag.Parse()

	server := signaling.NewSignalingServer(*addr)
	log.Printf("SignalingServer starting on %s", *addr)
	log.Fatal(server.Start())
}
