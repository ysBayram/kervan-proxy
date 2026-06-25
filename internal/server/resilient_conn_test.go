package server

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestResilientUpstreamFlow(t *testing.T) {
	proxyAddr := "127.0.0.1:21000"
	backendAddr := "127.0.0.1:21001"

	// Start proxy (with backend not running yet)
	cfg := DefaultConfig()
	cfg.ListenAddr = proxyAddr
	cfg.UpstreamAddr = backendAddr
	cfg.UpstreamTimeout = 500 * time.Millisecond

	srv := NewProxyServer(cfg)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	time.Sleep(50 * time.Millisecond)

	// 1. Connect client to proxy. The client should connect successfully even though backend is down.
	u := fmt.Sprintf("ws://%s/ws", proxyAddr)
	clientConn, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatalf("expected successful client connection: %v", err)
	}
	defer clientConn.Close()

	// 2. Send message while backend is down
	msg1 := "message-buffered-while-down"
	if err := clientConn.WriteMessage(websocket.TextMessage, []byte(msg1)); err != nil {
		t.Fatal(err)
	}

	// 3. Now start the backend (an echo server)
	backendListener, err := net.Listen("tcp", backendAddr)
	if err != nil {
		t.Fatal(err)
	}
	
	backendReceived := make(chan string, 1)
	go func() {
		conn, err := backendListener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Read and echo back
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		backendReceived <- string(buf[:n])
		conn.Write(buf[:n])
	}()

	// 4. Verify that the buffered message is received by backend
	select {
	case rec := <-backendReceived:
		if rec != msg1 {
			t.Fatalf("expected backend to receive %q, got %q", msg1, rec)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for backend to receive buffered message")
	}

	// 5. Verify the client gets the echo response
	_, reply, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(reply) != msg1 {
		t.Fatalf("expected client to receive echo %q, got %q", msg1, string(reply))
	}

	// Clean up backend for first part
	backendListener.Close()
	time.Sleep(200 * time.Millisecond)

	// 6. Send message again while backend is down again (simulated crash)
	msg2 := "message-buffered-during-crash"
	if err := clientConn.WriteMessage(websocket.TextMessage, []byte(msg2)); err != nil {
		t.Fatal(err)
	}

	// 7. Restart backend
	backendListener2, err := net.Listen("tcp", backendAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer backendListener2.Close()

	backendReceived2 := make(chan string, 1)
	go func() {
		conn, err := backendListener2.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		backendReceived2 <- string(buf[:n])
		conn.Write(buf[:n])
	}()

	// 8. Verify second message is delivered to new backend instance
	select {
	case rec := <-backendReceived2:
		if rec != msg2 {
			t.Fatalf("expected second backend to receive %q, got %q", msg2, rec)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for backend to receive second message")
	}

	// 9. Verify client receives second echo
	_, reply2, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if string(reply2) != msg2 {
		t.Fatalf("expected client to receive second echo %q, got %q", msg2, string(reply2))
	}
}
