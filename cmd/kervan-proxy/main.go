package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ysBayram/kervan-proxy/internal/server"
)

func main() {
	cfg := server.ConfigFromFlags()
	srv := server.NewProxyServer(cfg)

	if err := srv.Start(); err != nil {
		log.Fatalf("server: failed to start: %v", err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	srv.Stop()
}
