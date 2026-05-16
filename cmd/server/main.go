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
	dbPath := flag.String("db", "im.db", "SQLite database path")
	flag.Parse()

	server, err := engine.New(engine.Config{
		Addr:   *addr,
		DBPath: *dbPath,
	})
	if err != nil {
		panic(err)
	}
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
