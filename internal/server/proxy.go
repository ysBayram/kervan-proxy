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
	"github.com/ysBayram/kervan-proxy/internal/interceptor"
	"github.com/ysBayram/kervan-proxy/internal/pipeline"
	"github.com/ysBayram/kervan-proxy/internal/sanctuary"
	"github.com/ysBayram/kervan-proxy/internal/valve"
)

type ProxyServer struct {
	cfg    Config
	ctx    context.Context
	cancel context.CancelFunc

	httpServer  *http.Server
	tcpListener net.Listener

	activeConns   sync.WaitGroup
	activeWSConns sync.Map
	connCounter   atomic.Int64

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
	s.tcpListener = httpListener

	s.httpServer = &http.Server{
		Handler:      mux,
		ReadTimeout:  s.cfg.ReadTimeout,
		WriteTimeout: s.cfg.WriteTimeout,
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

	s.activeWSConns.Range(func(key, value any) bool {
		wsConn := value.(*websocket.Conn)
		wsConn.SetReadDeadline(time.Now())
		return true
	})

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

	connID := s.connCounter.Load()
	s.activeWSConns.Store(connID, wsConn)
	s.activeConns.Add(1)

	clientReader, clientWriter, closeAdapter := newWSAdapter(wsConn)

	defer func() {
		s.activeWSConns.Delete(connID)
		closeAdapter()
		s.activeConns.Done()
	}()

	ru := NewResilientUpstream(s.ctx, s.cfg.UpstreamAddr, s.cfg.UpstreamTimeout, s.cfg.ReconnectTimeout)
	defer ru.Close()

	s.runPipelines(clientReader, clientWriter, ru, ru)
}

type cancelReader struct {
	r      io.Reader
	cancel context.CancelFunc
	ctx    context.Context
}

func (cr *cancelReader) Read(p []byte) (int, error) {
	select {
	case <-cr.ctx.Done():
		return 0, cr.ctx.Err()
	default:
	}

	n, err := cr.r.Read(p)
	if err != nil {
		cr.cancel()
	}
	return n, err
}

func (s *ProxyServer) runPipelines(
	clientReader io.Reader, clientWriter io.Writer,
	upReader io.Reader, upWriter io.Writer,
) {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	wrappedClientReader := &cancelReader{r: clientReader, cancel: cancel, ctx: ctx}
	wrappedUpReader := &cancelReader{r: upReader, cancel: cancel, ctx: ctx}

	sancCap := s.cfg.SanctuaryCap
	if sancCap <= 0 {
		sancCap = 10000
	}

	ingress := pipeline.NewPipeline(wrappedClientReader, upWriter,
		pipeline.WithSanctuaryCapacity(sancCap),
	)
	egress := pipeline.NewPipeline(wrappedUpReader, clientWriter,
		pipeline.WithSanctuaryCapacity(sancCap),
	)

	s.configureBackpressure(ingress)
	s.configureBackpressure(egress)

	if ru, ok := upWriter.(*ResilientUpstream); ok {
		if !ru.IsConnected() {
			ingress.Valve().TransitionTo(valve.HELD)
		}
		ingress.RegisterRule(interceptor.Rule{
			Name: "reconnect-and-drain",
			Predicate: func() bool {
				return ru.IsConnected() && ingress.Valve().State() == valve.HELD
			},
			Action: func() {
				ingress.Valve().TransitionTo(valve.DRAINING)
			},
		})
	}

	if err := ingress.Start(); err != nil {
		log.Printf("server: ingress start error: %v", err)
		return
	}
	if err := egress.Start(); err != nil {
		ingress.Stop(0)
		log.Printf("server: egress start error: %v", err)
		return
	}

	<-ctx.Done()

	ingress.Stop(s.cfg.ShutdownDrainTimeout)

	if ru, ok := upWriter.(*ResilientUpstream); ok {
		ru.Close()
	}

	egress.Stop(0)
}

func (s *ProxyServer) configureBackpressure(p *pipeline.Pipeline) {
	switch s.cfg.BackpressureMode {
	case "reject_new":
		p.SetBackpressure(sanctuary.RejectNew)
	case "drop_connection":
		p.SetBackpressure(sanctuary.DropConnection)
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

func (s *ProxyServer) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":      "ok",
		"connections": s.connCounter.Load(),
		"uptime":      time.Since(s.startedAt).String(),
	})
}
