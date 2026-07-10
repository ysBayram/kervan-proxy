package server

import (
	"flag"
	"os"
	"strconv"
	"time"
)

const (
	// UpstreamModeTCP bridges the client WebSocket to a raw TCP backend
	// (the original behavior): the upstream is dialed as a plain TCP socket
	// and payload bytes are streamed through untouched.
	UpstreamModeTCP = "tcp"
	// UpstreamModeWS makes the proxy a transparent WebSocket reverse proxy:
	// the incoming request path and subprotocol are forwarded and the
	// upstream is dialed as a WebSocket (ws:// / wss://). This is what an
	// OCPP CSMS (path /ocpp/{cpID}, subprotocol ocpp1.6) requires.
	UpstreamModeWS = "ws"
)

type Config struct {
	ListenAddr           string
	UpstreamAddr         string
	UpstreamMode         string
	SanctuaryCap         int
	ReadBufferSize       int
	BackpressureMode     string
	MaxConnections       int
	UpstreamTimeout      time.Duration
	ReadTimeout          time.Duration
	WriteTimeout         time.Duration
	ReconnectTimeout     time.Duration
	ShutdownDrainTimeout time.Duration
	DatabaseURL          string
}

func DefaultConfig() Config {
	return Config{
		ListenAddr:           ":8080",
		UpstreamAddr:         "127.0.0.1:9000",
		UpstreamMode:         UpstreamModeTCP,
		SanctuaryCap:         10000,
		ReadBufferSize:       65536,
		BackpressureMode:     "drop_oldest",
		MaxConnections:       10000,
		UpstreamTimeout:      10 * time.Second,
		ReadTimeout:          30 * time.Second,
		WriteTimeout:         30 * time.Second,
		ReconnectTimeout:     30 * time.Second,
		ShutdownDrainTimeout: 30 * time.Second,
		DatabaseURL:          "postgres://localhost:5432/kervan?sslmode=disable",
	}
}

func ConfigFromFlags() Config {
	cfg := DefaultConfig()

	flag.StringVar(&cfg.ListenAddr, "listen", envStr("LISTEN", cfg.ListenAddr), "Listen address (host:port)")
	flag.StringVar(&cfg.UpstreamAddr, "upstream", envStr("UPSTREAM", cfg.UpstreamAddr), "Upstream backend address: host:port (tcp mode) or ws://host:port[/path] (ws mode)")
	flag.StringVar(&cfg.UpstreamMode, "upstream-mode", envStr("UPSTREAM_MODE", cfg.UpstreamMode), "Upstream mode: tcp (raw TCP bridge) | ws (WebSocket reverse proxy, forwards path+subprotocol)")
	flag.IntVar(&cfg.SanctuaryCap, "capacity", envInt("CAPACITY", cfg.SanctuaryCap), "Sanctuary buffer capacity per pipeline")
	flag.IntVar(&cfg.ReadBufferSize, "read-buffer-size", envInt("READ_BUFFER_SIZE", cfg.ReadBufferSize), "Per-read byte buffer; in ws mode this bounds the largest single message forwarded without splitting")
	flag.StringVar(&cfg.BackpressureMode, "backpressure", envStr("BACKPRESSURE", cfg.BackpressureMode), "Backpressure mode (drop_oldest|reject_new)")
	flag.IntVar(&cfg.MaxConnections, "max-connections", envInt("MAX_CONNECTIONS", cfg.MaxConnections), "Maximum concurrent connections")
	flag.DurationVar(&cfg.UpstreamTimeout, "upstream-timeout", envDur("UPSTREAM_TIMEOUT", cfg.UpstreamTimeout), "Upstream dial timeout")
	flag.DurationVar(&cfg.ReconnectTimeout, "reconnect-timeout", envDur("RECONNECT_TIMEOUT", cfg.ReconnectTimeout), "Upstream reconnect timeout (0 = unlimited)")
	flag.DurationVar(&cfg.ShutdownDrainTimeout, "shutdown-drain-timeout", envDur("SHUTDOWN_DRAIN_TIMEOUT", cfg.ShutdownDrainTimeout), "Pipeline drain timeout on shutdown")
	flag.DurationVar(&cfg.ReadTimeout, "read-timeout", envDur("READ_TIMEOUT", cfg.ReadTimeout), "Read timeout")
	flag.DurationVar(&cfg.WriteTimeout, "write-timeout", envDur("WRITE_TIMEOUT", cfg.WriteTimeout), "Write timeout")
	flag.StringVar(&cfg.DatabaseURL, "database-url", envStr("DATABASE_URL", cfg.DatabaseURL), "Database URL for monitoring")
	flag.Parse()

	return cfg
}

func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envDur(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
