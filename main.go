package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type config struct {
	Addr   string
	DBPath string
}

func defaultConfig() config {
	return config{
		Addr:   "127.0.0.1:8080",
		DBPath: "notifyd.db",
	}
}

func configFromEnv() config {
	cfg := defaultConfig()
	if value := os.Getenv("NOTIFYD_ADDR"); value != "" {
		cfg.Addr = value
	}
	if value := os.Getenv("NOTIFYD_DB_PATH"); value != "" {
		cfg.DBPath = value
	}
	return cfg
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg := configFromEnv()
	service, err := newService(cfg)
	if err != nil {
		return fmt.Errorf("start notifyd: %w", err)
	}
	defer service.close()

	server := &http.Server{
		Addr:              cfg.Addr,
		Handler:           service.handler(),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("notifyd listening on %s", cfg.Addr)
	select {
	case err := <-serverErrors:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}
