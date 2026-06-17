package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ysBayram/kervan-proxy/internal/pipeline"
	"github.com/ysBayram/kervan-proxy/internal/sanctuary"
)

type ProxyServer struct {
	cfg    Config
	ctx    context.Context
	cancel context.CancelFunc

	httpServer  *http.Server
	tcpListener net.Listener

	activeConns sync.WaitGroup
	connCounter atomic.Int64

	startedAt time.Time
	upgrader  websocket.Upgrader
}

func NewProxyServer(cfg Config) *ProxyServer {
	return &ProxyServer{
		cfg: cfg,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
	}
}

func (s *ProxyServer) Start() error {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.startedAt = time.Now()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/healthz", s.handleHealth)

	httpListener, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("server: listen %s: %w", s.cfg.ListenAddr, err)
	}

	s.httpServer = &http.Server{
		Handler:     mux,
		ReadTimeout: s.cfg.ReadTimeout,
	}

	go func() {
		if err := s.httpServer.Serve(httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			log.Printf("server: http serve error: %v", err)
		}
	}()

	log.Printf("server: listening on %s (WS/HTTP), upstream %s, max-connections %d",
		s.cfg.ListenAddr, s.cfg.UpstreamAddr, s.cfg.MaxConnections)
	return nil
}

func (s *ProxyServer) Stop() {
	log.Println("server: shutting down...")
	s.cancel()

	if s.httpServer != nil {
		s.httpServer.Close()
	}
	if s.tcpListener != nil {
		s.tcpListener.Close()
	}

	s.activeConns.Wait()
	log.Println("server: all connections closed")
}

func (s *ProxyServer) handleWS(w http.ResponseWriter, r *http.Request) {
	if !s.acquireConn() {
		http.Error(w, "connection limit reached", http.StatusServiceUnavailable)
		return
	}
	defer s.releaseConn()

	wsConn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("server: ws upgrade error: %v", err)
		return
	}
	defer wsConn.Close()

	upConn, err := s.dialUpstream()
	if err != nil {
		log.Printf("server: upstream dial error: %v", err)
		wsConn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "upstream unavailable"))
		return
	}
	defer upConn.Close()

	clientReader, clientWriter := newWSAdapter(wsConn)
	s.runPipelines(clientReader, clientWriter, upConn, upConn)
}

func (s *ProxyServer) runPipelines(
	clientReader io.Reader, clientWriter io.Writer,
	upReader io.Reader, upWriter io.Writer,
) {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	sancCap := s.cfg.SanctuaryCap
	if sancCap <= 0 {
		sancCap = 10000
	}

	ingress := pipeline.NewPipeline(clientReader, upWriter,
		pipeline.WithSanctuaryCapacity(sancCap),
	)
	egress := pipeline.NewPipeline(upReader, clientWriter,
		pipeline.WithSanctuaryCapacity(sancCap),
	)

	s.configureBackpressure(ingress)
	s.configureBackpressure(egress)

	if err := ingress.Start(); err != nil {
		log.Printf("server: ingress start error: %v", err)
		return
	}
	if err := egress.Start(); err != nil {
		ingress.Stop()
		log.Printf("server: egress start error: %v", err)
		return
	}

	<-ctx.Done()

	ingress.Stop()
	egress.Stop()
}

func (s *ProxyServer) configureBackpressure(p *pipeline.Pipeline) {
	switch s.cfg.BackpressureMode {
	case "reject_new":
		p.SetBackpressure(sanctuary.RejectNew)
	default:
		p.SetBackpressure(sanctuary.DropOldest)
	}
}

func (s *ProxyServer) acquireConn() bool {
	for {
		current := s.connCounter.Load()
		if current >= int64(s.cfg.MaxConnections) {
			return false
		}
		if s.connCounter.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (s *ProxyServer) releaseConn() {
	s.connCounter.Add(-1)
}

func (s *ProxyServer) dialUpstream() (net.Conn, error) {
	dialer := net.Dialer{Timeout: s.cfg.UpstreamTimeout}
	return dialer.DialContext(s.ctx, "tcp", s.cfg.UpstreamAddr)
}

func (s *ProxyServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":      "ok",
		"connections": s.connCounter.Load(),
		"uptime":      time.Since(s.startedAt).String(),
	})
}
