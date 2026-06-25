package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type ResilientUpstream struct {
	addr        string
	dialTimeout time.Duration
	ctx         context.Context
	cancel      context.CancelFunc

	mu        sync.Mutex
	conn      net.Conn
	connected bool
	cond      *sync.Cond
}

func NewResilientUpstream(ctx context.Context, addr string, timeout time.Duration) *ResilientUpstream {
	parentCtx, cancel := context.WithCancel(ctx)
	ru := &ResilientUpstream{
		addr:        addr,
		dialTimeout: timeout,
		ctx:         parentCtx,
		cancel:      cancel,
	}
	ru.cond = sync.NewCond(&ru.mu)

	// Start initial connection loop
	go ru.reconnectLoop()
	return ru
}

func (ru *ResilientUpstream) reconnectLoop() {
	for {
		select {
		case <-ru.ctx.Done():
			return
		default:
		}

		dialer := net.Dialer{Timeout: ru.dialTimeout}
		conn, err := dialer.DialContext(ru.ctx, "tcp", ru.addr)
		if err == nil {
			ru.mu.Lock()
			ru.conn = conn
			ru.connected = true
			ru.cond.Broadcast()
			ru.mu.Unlock()
			log.Printf("resilient-upstream: successfully connected/reconnected to %s", ru.addr)
			return
		}

		log.Printf("resilient-upstream: connection to %s failed: %v. Retrying in 1s...", ru.addr, err)
		select {
		case <-ru.ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
	}
}

func (ru *ResilientUpstream) Read(p []byte) (int, error) {
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

		n, err := conn.Read(p)
		if err != nil {
			ru.mu.Lock()
			if ru.connected {
				log.Printf("resilient-upstream: read error: %v. Reconnecting...", err)
				ru.connected = false
				if ru.conn != nil {
					ru.conn.Close()
					ru.conn = nil
				}
				go ru.reconnectLoop()
			}
			ru.mu.Unlock()
			continue
		}
		return n, nil
	}
}

func (ru *ResilientUpstream) Write(p []byte) (int, error) {
	ru.mu.Lock()
	if !ru.connected {
		ru.mu.Unlock()
		return 0, fmt.Errorf("upstream disconnected")
	}
	conn := ru.conn
	ru.mu.Unlock()

	n, err := conn.Write(p)
	if err != nil {
		ru.mu.Lock()
		if ru.connected {
			log.Printf("resilient-upstream: write error: %v. Reconnecting...", err)
			ru.connected = false
			if ru.conn != nil {
				ru.conn.Close()
				ru.conn = nil
			}
			go ru.reconnectLoop()
		}
		ru.mu.Unlock()
		return n, err
	}
	return n, nil
}

func (ru *ResilientUpstream) Close() error {
	ru.cancel()
	ru.mu.Lock()
	defer ru.mu.Unlock()
	ru.cond.Broadcast()
	if ru.conn != nil {
		err := ru.conn.Close()
		ru.conn = nil
		return err
	}
	return nil
}

func (ru *ResilientUpstream) IsConnected() bool {
	ru.mu.Lock()
	defer ru.mu.Unlock()
	return ru.connected
}
