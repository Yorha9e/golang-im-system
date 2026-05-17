package main

import (
	"flag"
	"log"
	"os"

	"github.com/Yorha9e/golang-im-system/signaling"
)

func main() {
	addr := flag.String("addr", ":7000", "listen address ($SIGNALING_ADDR)")
	flag.Parse()

	bind := *addr
	if v := os.Getenv("SIGNALING_ADDR"); v != "" {
		bind = v
	}

	server := signaling.NewSignalingServer(bind)
	log.Printf("SignalingServer starting on %s", bind)
	log.Fatal(server.Start())
}
