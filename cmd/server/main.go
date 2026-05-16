package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"golang-im-system/internal/engine"
)

func main() {
	addr := flag.String("addr", ":8888", "listen address")
	flag.Parse()

	server := engine.New(*addr)
	go func() {
		if err := server.Start(); err != nil {
			panic(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	server.Stop(context.Background())
}
