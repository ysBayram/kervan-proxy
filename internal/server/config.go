package server

import (
	"flag"
	"os"
	"strconv"
	"time"
)

type Config struct {
	ListenAddr       string
	UpstreamAddr     string
	SanctuaryCap     int
	BackpressureMode string
	MaxConnections   int
	UpstreamTimeout  time.Duration
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	DatabaseURL      string
}

func DefaultConfig() Config {
	return Config{
		ListenAddr:       ":8080",
		UpstreamAddr:     "127.0.0.1:9000",
		SanctuaryCap:     10000,
		BackpressureMode: "drop_oldest",
		MaxConnections:   10000,
		UpstreamTimeout:  10 * time.Second,
		ReadTimeout:      30 * time.Second,
		WriteTimeout:     30 * time.Second,
		DatabaseURL:      "postgres://localhost:5432/kervan?sslmode=disable",
	}
}

func ConfigFromFlags() Config {
	cfg := DefaultConfig()

	flag.StringVar(&cfg.ListenAddr, "listen", envStr("LISTEN", cfg.ListenAddr), "Listen address (host:port)")
	flag.StringVar(&cfg.UpstreamAddr, "upstream", envStr("UPSTREAM", cfg.UpstreamAddr), "Upstream backend address (host:port)")
	flag.IntVar(&cfg.SanctuaryCap, "capacity", envInt("CAPACITY", cfg.SanctuaryCap), "Sanctuary buffer capacity per pipeline")
	flag.StringVar(&cfg.BackpressureMode, "backpressure", envStr("BACKPRESSURE", cfg.BackpressureMode), "Backpressure mode (drop_oldest|reject_new)")
	flag.IntVar(&cfg.MaxConnections, "max-connections", envInt("MAX_CONNECTIONS", cfg.MaxConnections), "Maximum concurrent connections")
	flag.DurationVar(&cfg.UpstreamTimeout, "upstream-timeout", envDur("UPSTREAM_TIMEOUT", cfg.UpstreamTimeout), "Upstream dial timeout")
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
