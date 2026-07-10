package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHealthEndpoint(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenAddr = ":20991"
	cfg.UpstreamAddr = ":20992"

	srv := NewProxyServer(cfg)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("http://localhost%s/healthz", cfg.ListenAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status ok, got %v", body["status"])
	}
}

func TestConnectionLimit(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ListenAddr = ":20995"
	cfg.UpstreamAddr = ":20996"
	cfg.MaxConnections = 1

	srv := NewProxyServer(cfg)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get(fmt.Sprintf("http://localhost%s/healthz", cfg.ListenAddr))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for first conn, got %d", resp.StatusCode)
	}
}

func TestWebSocketEcho(t *testing.T) {
	echoAddr := ":20993"
	proxyAddr := ":20994"

	echoDone := make(chan struct{})
	echoListener, err := net.Listen("tcp", echoAddr)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		defer close(echoDone)
		for {
			conn, err := echoListener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c)
			}(conn)
		}
	}()
	defer echoListener.Close()

	cfg := DefaultConfig()
	cfg.ListenAddr = proxyAddr
	cfg.UpstreamAddr = echoAddr
	cfg.MaxConnections = 10

	srv := NewProxyServer(cfg)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	time.Sleep(100 * time.Millisecond)

	u := fmt.Sprintf("ws://localhost%s/ws", proxyAddr)
	conn, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	msg := "hello-kervan"
	if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
		t.Fatal(err)
	}

	_, reply, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(reply) != msg {
		t.Fatalf("expected %q, got %q", msg, string(reply))
	}
}
