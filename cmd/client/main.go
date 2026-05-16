package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"golang-im-system/internal/service"
	"golang-im-system/internal/transport"
)

var (
	mode      = flag.String("mode", "server", "transport mode: server | lan_p2p | wan_p2p")
	server    = flag.String("server", "127.0.0.1:8888", "ChatServer address")
	signaling = flag.String("signaling", "127.0.0.1:7000", "SignalingServer address")
	name      = flag.String("name", "", "display name")
	port      = flag.Int("port", 9000, "local WS port for P2P modes")
)

func main() {
	flag.Parse()

	displayName := *name
	if displayName == "" {
		fmt.Print("Enter your name: ")
		fmt.Scanln(&displayName)
	}

	token := login(*server, displayName)

	t := createTransport(displayName, token)
	if t == nil {
		fmt.Fprintf(os.Stderr, "failed to create transport for mode %q\n", *mode)
		os.Exit(1)
	}

	session := service.NewSession(t)
	session.SetOnReceive(onReceive)
	session.SetOnError(func(err error) {
		fmt.Fprintf(os.Stderr, "\n! error: %v\n> ", err)
	})

	if err := session.Start(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "failed to start session: %v\n", err)
		os.Exit(1)
	}
	defer session.Stop()

	printBanner(*mode)

	// Stdin reader
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			dispatch(scanner.Text(), session)
			fmt.Print("> ")
		}
	}()

	select {}
}

func login(serverAddr, username string) string {
	resp, err := http.Get(fmt.Sprintf("http://%s/login?user=%s", serverAddr, username))
	if err != nil {
		fmt.Fprintf(os.Stderr, "login failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var result struct{ Token string }
	json.Unmarshal(body, &result)
	if result.Token == "" {
		fmt.Fprintf(os.Stderr, "login failed: no token returned\n")
		os.Exit(1)
	}
	return result.Token
}

func createTransport(displayName, token string) transport.Transport {
	switch *mode {
	case "server":
		return transport.NewServerTransport(*server, token)

	case "lan_p2p":
		// return transport.NewLANP2PTransport(displayName, *port)
		fmt.Println("LAN P2P mode not yet wired")
		os.Exit(1)
		return nil

	case "wan_p2p":
		// wan := transport.NewWANP2PTransport(displayName, *signaling, *server)
		// wan.OnFallback = func() { fmt.Println("\n>>> fallback to server <<<\n> ") }
		// return wan
		fmt.Println("WAN P2P mode not yet wired")
		os.Exit(1)
		return nil

	default:
		return nil
	}
}

func dispatch(input string, s *service.Session) {
	switch {
	case input == "/who":
		s.Who()

	case strings.HasPrefix(input, "/rename "):
		n := strings.TrimPrefix(input, "/rename ")
		s.Rename(n)

	case strings.HasPrefix(input, "/burn "):
		rest := strings.TrimPrefix(input, "/burn ")
		parts := strings.SplitN(rest, " ", 2)
		if len(parts) != 2 {
			fmt.Println("usage: /burn <seconds> <message>")
			return
		}
		var sec int32
		fmt.Sscanf(parts[0], "%d", &sec)
		s.Send(parts[1], sec)

	case strings.HasPrefix(input, "/pm "):
		rest := strings.TrimPrefix(input, "/pm ")
		parts := strings.SplitN(rest, " ", 2)
		if len(parts) != 2 {
			fmt.Println("usage: /pm <name> <message>")
			return
		}
		s.PrivateSend(parts[0], parts[1], 0)

	case strings.HasPrefix(input, "/mode "):
		fmt.Println("mode switch not yet implemented: restart with --mode flag")

	case input == "/quit":
		fmt.Println("bye.")
		os.Exit(0)

	default:
		if strings.TrimSpace(input) != "" {
			s.Send(input, 0)
		}
	}
}

func onReceive(d service.DisplayMsg) {
	switch d.Type {
	case "chat":
		fmt.Printf("\r[%s] %s\n> ", d.From, d.Content)
	case "private":
		fmt.Printf("\r[PM from %s] %s\n> ", d.From, d.Content)
	case "system":
		fmt.Printf("\r--- %s ---\n> ", d.Content)
	case "burn":
		if d.MessageID != "" {
			fmt.Printf("\r[burned: %s...]\n> ", d.MessageID[:8])
		}
	default:
		fmt.Printf("\r[%s] %s\n> ", d.Type, d.Content)
	}
}

func printBanner(mode string) {
	modeLabel := map[string]string{
		"server":   "Server (centralized)",
		"lan_p2p":  "LAN P2P",
		"wan_p2p":  "WAN P2P",
	}[mode]
	fmt.Printf("=== IM Chat [%s] ===\n", modeLabel)
	fmt.Println("/who | /rename <name> | /pm <name> <msg> | /burn <sec> <msg> | /mode <type> | /quit")
	fmt.Println()
	fmt.Print("> ")
}
