package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ResilientWSUpstream is the WebSocket counterpart of ResilientUpstream. It
// dials the upstream as a WebSocket (forwarding the negotiated OCPP subprotocol)
// and exposes an io.Reader/io.Writer over WebSocket text messages, while keeping
// the same reconnect semantics so the Sanctuary/valve resilience machinery works
// unchanged. Each Write emits exactly one WebSocket message; each Read returns
// the bytes of exactly one WebSocket message (buffering any remainder that does
// not fit the caller's slice), preserving OCPP-J message boundaries.
type ResilientWSUpstream struct {
	url              string
	subprotocols     []string
	dialTimeout      time.Duration
	reconnectTimeout time.Duration
	ctx              context.Context
	cancel           context.CancelFunc

	mu               sync.Mutex
	conn             *websocket.Conn
	connected        bool
	reconnecting     atomic.Bool
	cond             *sync.Cond
	reconnectStarted time.Time

	readBuf []byte // leftover of a message larger than the caller's slice (Read only)
}

func NewResilientWSUpstream(ctx context.Context, url string, subprotocols []string, dialTimeout, reconnectTimeout time.Duration) *ResilientWSUpstream {
	parentCtx, cancel := context.WithCancel(ctx)
	ru := &ResilientWSUpstream{
		url:              url,
		subprotocols:     subprotocols,
		dialTimeout:      dialTimeout,
		reconnectTimeout: reconnectTimeout,
		ctx:              parentCtx,
		cancel:           cancel,
	}
	ru.cond = sync.NewCond(&ru.mu)

	go ru.reconnectLoop()
	return ru
}

func (ru *ResilientWSUpstream) reconnectLoop() {
	defer ru.reconnecting.Store(false)

	ru.mu.Lock()
	ru.reconnectStarted = time.Now()
	ru.mu.Unlock()

	dialer := websocket.Dialer{
		HandshakeTimeout: ru.dialTimeout,
		Subprotocols:     ru.subprotocols,
	}

	for {
		select {
		case <-ru.ctx.Done():
			return
		default:
		}

		conn, _, err := dialer.DialContext(ru.ctx, ru.url, nil)
		if err == nil {
			ru.mu.Lock()
			ru.conn = conn
			ru.connected = true
			ru.cond.Broadcast()
			ru.mu.Unlock()
			log.Printf("resilient-ws-upstream: connected/reconnected to %s (subprotocol %q)", ru.url, conn.Subprotocol())
			return
		}

		log.Printf("resilient-ws-upstream: connection to %s failed: %v. Retrying in %s...", ru.url, err, reconnectRetryDelay)

		ru.mu.Lock()
		timeout := ru.reconnectTimeout
		started := ru.reconnectStarted
		ru.mu.Unlock()

		if timeout > 0 && time.Since(started) >= timeout {
			log.Printf("resilient-ws-upstream: reconnect timeout (%v) exceeded for %s, giving up", timeout, ru.url)
			ru.cancel()
			return
		}

		select {
		case <-ru.ctx.Done():
			return
		case <-time.After(reconnectRetryDelay):
		}
	}
}

func (ru *ResilientWSUpstream) Read(p []byte) (int, error) {
	// Drain any leftover from a previously oversized message first.
	if len(ru.readBuf) > 0 {
		n := copy(p, ru.readBuf)
		ru.readBuf = ru.readBuf[n:]
		return n, nil
	}

	for {
		ru.mu.Lock()
		for !ru.connected && ru.ctx.Err() == nil {
			ru.cond.Wait()
		}
		if ru.ctx.Err() != nil {
			ru.mu.Unlock()
			return 0, io.EOF
		}
		conn := ru.conn
		ru.mu.Unlock()

		_, msg, err := conn.ReadMessage()
		if err != nil {
			ru.handleConnErr("read", err, conn)
			continue
		}

		n := copy(p, msg)
		if n < len(msg) {
			ru.readBuf = append(ru.readBuf[:0], msg[n:]...)
		}
		return n, nil
	}
}

func (ru *ResilientWSUpstream) Write(p []byte) (int, error) {
	ru.mu.Lock()
	if !ru.connected {
		ru.mu.Unlock()
		return 0, fmt.Errorf("upstream disconnected")
	}
	conn := ru.conn
	ru.mu.Unlock()

	conn.SetWriteDeadline(time.Now().Add(writeWait))
	if err := conn.WriteMessage(websocket.TextMessage, p); err != nil {
		ru.handleConnErr("write", err, conn)
		return 0, err
	}
	return len(p), nil
}

// handleConnErr marks the upstream disconnected and kicks off a reconnect,
// guarding against a racing goroutine that already swapped the connection.
func (ru *ResilientWSUpstream) handleConnErr(op string, err error, seen *websocket.Conn) {
	ru.mu.Lock()
	defer ru.mu.Unlock()
	if ru.connected && ru.conn == seen {
		log.Printf("resilient-ws-upstream: %s error: %v. Reconnecting...", op, err)
		ru.connected = false
		if ru.conn != nil {
			ru.conn.Close()
			ru.conn = nil
		}
		if ru.reconnecting.CompareAndSwap(false, true) {
			go ru.reconnectLoop()
		}
	}
}

func (ru *ResilientWSUpstream) Close() error {
	ru.cancel()
	ru.mu.Lock()
	defer ru.mu.Unlock()
	ru.connected = false
	ru.cond.Broadcast()
	if ru.conn != nil {
		err := ru.conn.Close()
		ru.conn = nil
		return err
	}
	return nil
}

func (ru *ResilientWSUpstream) IsConnected() bool {
	ru.mu.Lock()
	defer ru.mu.Unlock()
	return ru.connected
}
