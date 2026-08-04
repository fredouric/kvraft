package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/fredouric/kvraft/server"
	"github.com/fredouric/kvraft/store/kvraft"
	"github.com/fredouric/kvraft/store/sqlite"
)

type config struct {
	id        string
	raftAddr  string
	rpcAddr   string
	dataDir   string
	bootstrap bool
	join      string
}

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	var cfg config

	cmd := &cobra.Command{
		Use:          "kvraft",
		Short:        "A Raft-backed key-value store",
		SilenceUsage: true,
		RunE: func(_ *cobra.Command, _ []string) error {
			if cfg.dataDir == "" {
				cfg.dataDir = filepath.Join("data", cfg.id)
			}
			return runServe(cfg)
		},
	}

	f := cmd.Flags()
	f.StringVar(&cfg.id, "id", "", "unique node ID (required)")
	f.StringVar(&cfg.raftAddr, "raft-addr", "127.0.0.1:9001", "address for the Raft transport (node-to-node)")
	f.StringVar(&cfg.rpcAddr, "rpc-addr", ":8080", "address for the client RPC API")
	f.StringVar(&cfg.dataDir, "data-dir", "", "dir for raft log, snapshots, kv db (default: data/<id>)")
	f.BoolVar(&cfg.bootstrap, "bootstrap", false, "bootstrap a new cluster")
	f.StringVar(&cfg.join, "join", "", "RPC address of an existing node to join")

	cmd.MarkFlagRequired("id")

	return cmd
}

func runServe(cfg config) error {
	if err := os.MkdirAll(cfg.dataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	s, err := sqlite.New(filepath.Join(cfg.dataDir, "kv.db"))
	if err != nil {
		return fmt.Errorf("init store: %w", err)
	}

	node, err := kvraft.NewNode(s, cfg.id, cfg.raftAddr, cfg.dataDir)
	if err != nil {
		return fmt.Errorf("init raft node: %w", err)
	}

	switch {
	case cfg.bootstrap:
		if err := node.Bootstrap(); err != nil {
			return fmt.Errorf("bootstrap cluster: %w", err)
		}
		if err := node.WaitForLeader(10 * time.Second); err != nil {
			return fmt.Errorf("wait for leader: %w", err)
		}
		slog.Info("cluster bootstrapped", "id", cfg.id, "raft", cfg.raftAddr)
	default:
		slog.Info("started without bootstrap; awaiting cluster membership", "id", cfg.id, "join", cfg.join)
	}

	kv := &server.KVService{Store: node}
	srv := server.New(cfg.rpcAddr, kv)
	if err := srv.Listen(); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	slog.Info("listening", "rpc", srv.Addr(), "raft", cfg.raftAddr)

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
	return nil
}
