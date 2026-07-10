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
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ysBayram/kervan-proxy/internal/interceptor"
	"github.com/ysBayram/kervan-proxy/internal/pipeline"
	"github.com/ysBayram/kervan-proxy/internal/sanctuary"
	"github.com/ysBayram/kervan-proxy/internal/valve"
)

const (
	wsEndpoint       = "/ws"
	healthEndpoint   = "/healthz"
	rootEndpoint     = "/"
	networkTCP       = "tcp"
	defaultSancCap   = 10000
	defaultWSBufSize = 4096
)

// upstream is the resilient backend connection driven by runPipelines. Both the
// raw-TCP (*ResilientUpstream) and WebSocket (*ResilientWSUpstream) backends
// satisfy it, so the pipeline wiring is identical for both modes.
type upstream interface {
	io.Reader
	io.Writer
	IsConnected() bool
	Close() error
}

type ProxyServer struct {
	cfg    Config
	ctx    context.Context
	cancel context.CancelFunc

	httpServer  *http.Server
	tcpListener net.Listener

	activeConns   sync.WaitGroup
	activeWSConns sync.Map
	connCounter   atomic.Int64
	connIDSeq     atomic.Int64

	startedAt time.Time
	upgrader  websocket.Upgrader
}

func NewProxyServer(cfg Config) *ProxyServer {
	return &ProxyServer{
		cfg: cfg,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  defaultWSBufSize,
			WriteBufferSize: defaultWSBufSize,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
	}
}

func (s *ProxyServer) Start() error {
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.startedAt = time.Now()

	mux := http.NewServeMux()
	mux.HandleFunc(healthEndpoint, s.handleHealth)
	if s.cfg.UpstreamMode == UpstreamModeWS {
		// Transparent reverse proxy: accept any path (e.g. /ocpp/{cpID}) and
		// forward it to the upstream. /healthz stays the more specific match.
		mux.HandleFunc(rootEndpoint, s.handleWS)
	} else {
		mux.HandleFunc(wsEndpoint, s.handleWS)
	}

	httpListener, err := net.Listen(networkTCP, s.cfg.ListenAddr)
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

	log.Printf("server: listening on %s (WS/HTTP), upstream %s (mode=%s), max-connections %d",
		s.cfg.ListenAddr, s.cfg.UpstreamAddr, s.cfg.UpstreamMode, s.cfg.MaxConnections)
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

	wsMode := s.cfg.UpstreamMode == UpstreamModeWS

	upgrader := s.upgrader
	clientMsgType := websocket.BinaryMessage
	if wsMode {
		// Echo the client's requested subprotocols so the negotiated OCPP
		// version (e.g. ocpp1.6) is preserved end-to-end, and speak text
		// frames as OCPP-J requires.
		upgrader.Subprotocols = websocket.Subprotocols(r)
		clientMsgType = websocket.TextMessage
	}

	wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("server: ws upgrade error: %v", err)
		return
	}

	connID := s.connIDSeq.Add(1)
	s.activeConns.Add(1)
	s.activeWSConns.Store(connID, wsConn)

	clientReader, clientWriter, closeAdapter := newWSAdapter(wsConn, clientMsgType)

	defer func() {
		s.activeWSConns.Delete(connID)
		closeAdapter()
		s.activeConns.Done()
	}()

	var up upstream
	if wsMode {
		upURL, uerr := s.buildUpstreamURL(r)
		if uerr != nil {
			log.Printf("server: bad upstream url: %v", uerr)
			return
		}
		// Build X-Forwarded-* headers (append semantics)
		hdr := http.Header{}
		if prior := r.Header.Get("X-Forwarded-Host"); prior != "" {
			hdr.Set("X-Forwarded-Host", prior+", "+r.Host)
		} else {
			hdr.Set("X-Forwarded-Host", r.Host)
		}
		if ip, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			if prior := r.Header.Get("X-Forwarded-For"); prior != "" {
				hdr.Set("X-Forwarded-For", prior+", "+ip)
			} else {
				hdr.Set("X-Forwarded-For", ip)
			}
		}
		if r.TLS != nil {
			hdr.Set("X-Forwarded-Proto", "https")
		} else {
			hdr.Set("X-Forwarded-Proto", "http")
		}

		up = NewResilientWSUpstream(s.ctx, upURL, websocket.Subprotocols(r), hdr, s.cfg.UpstreamTimeout, s.cfg.ReconnectTimeout)
	} else {
		up = NewResilientUpstream(s.ctx, s.cfg.UpstreamAddr, s.cfg.UpstreamTimeout, s.cfg.ReconnectTimeout)
	}

	defer up.Close()

	s.runPipelines(clientReader, clientWriter, up)
}

// buildUpstreamURL derives the upstream WebSocket URL for a client request by
// joining the configured upstream base (scheme://host[/prefix]) with the
// incoming request path and query — making the proxy transparent to path-based
// routing such as OCPP's /ocpp/{cpID}. A bare host:port is assumed to be ws://.
func (s *ProxyServer) buildUpstreamURL(r *http.Request) (string, error) {
	base := s.cfg.UpstreamAddr
	if !strings.Contains(base, "://") {
		base = "ws://" + base
	}
	u, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	u.Path = singleJoiningSlash(u.Path, r.URL.Path)
	u.RawQuery = r.URL.RawQuery
	return u.String(), nil
}

func singleJoiningSlash(a, b string) string {
	aslash := strings.HasSuffix(a, "/")
	bslash := strings.HasPrefix(b, "/")
	switch {
	case aslash && bslash:
		return a + b[1:]
	case !aslash && !bslash:
		return a + "/" + b
	}
	return a + b
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
	up upstream,
) {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	wrappedClientReader := &cancelReader{r: clientReader, cancel: cancel, ctx: ctx}
	wrappedUpReader := &cancelReader{r: up, cancel: cancel, ctx: ctx}

	sancCap := s.cfg.SanctuaryCap
	if sancCap <= 0 {
		sancCap = defaultSancCap
	}

	ingress := pipeline.NewPipeline(wrappedClientReader, up,
		pipeline.WithSanctuaryCapacity(sancCap),
		pipeline.WithReadBufferSize(s.cfg.ReadBufferSize),
	)
	egress := pipeline.NewPipeline(wrappedUpReader, clientWriter,
		pipeline.WithSanctuaryCapacity(sancCap),
		pipeline.WithReadBufferSize(s.cfg.ReadBufferSize),
	)

	s.configureBackpressure(ingress)
	s.configureBackpressure(egress)

	if !up.IsConnected() {
		ingress.Valve().TransitionTo(valve.HELD)
	}
	ingress.RegisterRule(interceptor.Rule{
		Name: "reconnect-and-drain",
		Predicate: func() bool {
			return up.IsConnected() && ingress.Valve().State() == valve.HELD
		},
		Action: func() {
			ingress.Valve().TransitionTo(valve.DRAINING)
		},
	})

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
	up.Close()
	egress.Stop(0)
}

func (s *ProxyServer) configureBackpressure(p *pipeline.Pipeline) {
	switch s.cfg.BackpressureMode {
	case sanctuary.RejectNew.String():
		p.SetBackpressure(sanctuary.RejectNew)
	case sanctuary.DropConnection.String():
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
