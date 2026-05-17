package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/Yorha9e/golang-im-system/internal/engine"
)

func envOrFlag(envKey, flagVal string) string {
	if v := os.Getenv(envKey); v != "" {
		return v
	}
	return flagVal
}

func envIntOrFlag(envKey string, flagVal int) int {
	if v := os.Getenv(envKey); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return flagVal
}

func envFloatOrFlag(envKey string, flagVal float64) float64 {
	if v := os.Getenv(envKey); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return flagVal
}

func main() {
	addr := flag.String("addr", ":8888", "listen address ($ADDR)")
	dbPath := flag.String("db", "im.db", "SQLite database path ($DB_PATH)")
	jwtSecret := flag.String("jwt-secret", "", "JWT signing secret ($JWT_SECRET)")
	msgRate := flag.Float64("msg-rate", 5, "messages per second per client ($MSG_RATE)")
	msgBurst := flag.Int("msg-burst", 10, "max burst messages per client ($MSG_BURST)")
	flag.Parse()

	cfg := engine.Config{
		Addr:      envOrFlag("ADDR", *addr),
		DBPath:    envOrFlag("DB_PATH", *dbPath),
		JWTSecret: envOrFlag("JWT_SECRET", *jwtSecret),
		MsgRate:   envFloatOrFlag("MSG_RATE", *msgRate),
		MsgBurst:  envIntOrFlag("MSG_BURST", *msgBurst),
		Production: os.Getenv("PRODUCTION") == "true",
		Version:    envOrFlag("VERSION", "dev"),
	}

	server, err := engine.New(cfg)
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
