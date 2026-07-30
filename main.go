package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/fredouric/kvraft/server"
	"github.com/fredouric/kvraft/store/kvraft"
	"github.com/fredouric/kvraft/store/sqlite"
)

const (
	rpcAddr  = ":8080"
	raftAddr = "127.0.0.1:9001"
	nodeID   = "node1"
	dataDir  = "data/node1"
)

func main() {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		slog.Error("failed to create data dir", "error", err)
		os.Exit(1)
	}

	s, err := sqlite.New(filepath.Join(dataDir, "kv.db"))
	if err != nil {
		slog.Error("failed to init store", "error", err)
		os.Exit(1)
	}

	node, err := kvraft.NewNode(s, nodeID, raftAddr, dataDir)
	if err != nil {
		slog.Error("failed to init raft node", "error", err)
		os.Exit(1)
	}

	if err := node.Bootstrap(); err != nil {
		slog.Error("failed to bootstrap cluster", "error", err)
		os.Exit(1)
	}

	if err := node.WaitForLeader(10 * time.Second); err != nil {
		slog.Error("no leader elected", "error", err)
		os.Exit(1)
	}
	slog.Info("cluster ready", "leader", true)

	kv := &server.KVService{Store: node}
	srv := server.New(rpcAddr, kv)
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
	if err := node.Close(); err != nil {
		slog.Error("failed to close raft node", "error", err)
	}
	if err := s.Close(); err != nil {
		slog.Error("failed to close store", "error", err)
	}
}
