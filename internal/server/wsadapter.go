package server

import (
	"io"
	"sync"

	"github.com/gorilla/websocket"
)

type wsReader struct {
	conn *websocket.Conn
	mu   sync.Mutex
	buf  []byte
}

func (r *wsReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.buf) > 0 {
		n := copy(p, r.buf)
		r.buf = r.buf[n:]
		return n, nil
	}

	_, msg, err := r.conn.ReadMessage()
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
	conn    *websocket.Conn
	writeMu sync.Mutex
}

func (w *wsWriter) Write(p []byte) (int, error) {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	if err := w.conn.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

func newWSAdapter(conn *websocket.Conn) (io.Reader, io.Writer) {
	return &wsReader{conn: conn}, &wsWriter{conn: conn}
}
