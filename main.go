package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

type config struct {
	Addr                 string
	DBPath               string
	WorkerCount          int
	PerHostConcurrency   int
	MaxAttempts          int
	PollInterval         time.Duration
	BaseBackoff          time.Duration
	MaxBackoff           time.Duration
	LeaseDuration        time.Duration
	RequestTimeout       time.Duration
	AllowPrivateNetworks bool
}

func defaultConfig() config {
	return config{
		Addr:               "127.0.0.1:8080",
		DBPath:             "notifyd.db",
		WorkerCount:        8,
		PerHostConcurrency: 2,
		MaxAttempts:        12,
		PollInterval:       250 * time.Millisecond,
		BaseBackoff:        5 * time.Second,
		MaxBackoff:         15 * time.Minute,
		LeaseDuration:      30 * time.Second,
		RequestTimeout:     10 * time.Second,
	}
}

func configFromEnv() (config, error) {
	cfg := defaultConfig()
	if value := os.Getenv("NOTIFYD_ADDR"); value != "" {
		cfg.Addr = value
	}
	if value := os.Getenv("NOTIFYD_DB_PATH"); value != "" {
		cfg.DBPath = value
	}

	parseInt := func(name string, target *int) error {
		value := os.Getenv(name)
		if value == "" {
			return nil
		}
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 {
			return fmt.Errorf("%s must be a positive integer", name)
		}
		*target = parsed
		return nil
	}
	parseDuration := func(name string, target *time.Duration) error {
		value := os.Getenv(name)
		if value == "" {
			return nil
		}
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed <= 0 {
			return fmt.Errorf("%s must be a positive duration", name)
		}
		*target = parsed
		return nil
	}

	for _, item := range []struct {
		name   string
		target *int
	}{
		{"NOTIFYD_WORKERS", &cfg.WorkerCount},
		{"NOTIFYD_PER_HOST_CONCURRENCY", &cfg.PerHostConcurrency},
		{"NOTIFYD_MAX_ATTEMPTS", &cfg.MaxAttempts},
	} {
		if err := parseInt(item.name, item.target); err != nil {
			return config{}, err
		}
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
	}{
		{"NOTIFYD_POLL_INTERVAL", &cfg.PollInterval},
		{"NOTIFYD_BASE_BACKOFF", &cfg.BaseBackoff},
		{"NOTIFYD_MAX_BACKOFF", &cfg.MaxBackoff},
		{"NOTIFYD_LEASE_DURATION", &cfg.LeaseDuration},
		{"NOTIFYD_REQUEST_TIMEOUT", &cfg.RequestTimeout},
	} {
		if err := parseDuration(item.name, item.target); err != nil {
			return config{}, err
		}
	}
	if value := os.Getenv("NOTIFYD_ALLOW_PRIVATE_NETWORKS"); value != "" {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return config{}, fmt.Errorf("NOTIFYD_ALLOW_PRIVATE_NETWORKS must be a boolean")
		}
		cfg.AllowPrivateNetworks = parsed
	}
	return cfg, nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}
	service, err := newService(cfg)
	if err != nil {
		return fmt.Errorf("start notifyd: %w", err)
	}
	defer service.close()
	service.start()

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
