package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/fredouric/kvraft/server"
	"github.com/fredouric/kvraft/store/sqlite"
)

func main() {
	s, err := sqlite.New("kv.db")
	if err != nil {
		slog.Error("failed to init store", "error", err)
		os.Exit(1)
	}

	kv := &server.KVService{Store: s}

	srv := server.New(":8080", kv)
	if err = srv.Listen(); err != nil {
		slog.Error("failed to listen", "error", err)
		os.Exit(1)
	}
	slog.Info("listening", "addr", srv.Addr())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	slog.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("failed to shut down server", "error", err)
	}
	if err := s.Close(); err != nil {
		slog.Error("failed to close store", "error", err)
	}
}
