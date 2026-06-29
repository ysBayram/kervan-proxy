package server

import (
	"io"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	pingInterval = 30 * time.Second
	pongTimeout  = 60 * time.Second
	writeWait    = 10 * time.Second
)

type wsAdapter struct {
	conn      *websocket.Conn
	writeMu   sync.Mutex
	stopCh    chan struct{}
	closeOnce sync.Once
}

type wsReader struct {
	adapter *wsAdapter
	mu      sync.Mutex
	buf     []byte
}

func (r *wsReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.buf) > 0 {
		n := copy(p, r.buf)
		r.buf = r.buf[n:]
		return n, nil
	}

	r.adapter.conn.SetReadDeadline(time.Now().Add(pongTimeout))
	_, msg, err := r.adapter.conn.ReadMessage()
	if err != nil {
		return 0, err
	}

	n := copy(p, msg)
	if n < len(msg) {
		r.buf = make([]byte, len(msg)-n)
		copy(r.buf, msg[n:])
	}
	return n, nil
}

type wsWriter struct {
	adapter *wsAdapter
}

func (w *wsWriter) Write(p []byte) (int, error) {
	w.adapter.writeMu.Lock()
	defer w.adapter.writeMu.Unlock()

	if err := w.adapter.conn.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (a *wsAdapter) pingLoop() {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-a.stopCh:
			return
		case <-ticker.C:
		}

		a.writeMu.Lock()
		a.conn.SetWriteDeadline(time.Now().Add(writeWait))
		err := a.conn.WriteMessage(websocket.PingMessage, nil)
		a.writeMu.Unlock()
		if err != nil {
			return
		}
	}
}

func (a *wsAdapter) close() {
	a.closeOnce.Do(func() {
		close(a.stopCh)
		a.writeMu.Lock()
		defer a.writeMu.Unlock()
		a.conn.Close()
	})
}

func newWSAdapter(conn *websocket.Conn) (io.Reader, io.Writer, func()) {
	a := &wsAdapter{
		conn:   conn,
		stopCh: make(chan struct{}),
	}

	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pongTimeout))
		return nil
	})

	go a.pingLoop()

	return &wsReader{adapter: a}, &wsWriter{adapter: a}, a.close
}
