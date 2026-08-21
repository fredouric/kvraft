package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/rpc"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/fredouric/kvraft/kvapi"
	"github.com/fredouric/kvraft/migrate"
	"github.com/fredouric/kvraft/server"
	"github.com/fredouric/kvraft/shardctrl"
	"github.com/fredouric/kvraft/store/kvraft"
	"github.com/fredouric/kvraft/store/sqlite"
)

type config struct {
	id         string
	raftAddr   string
	rpcAddr    string
	dataDir    string
	bootstrap  bool
	join       string
	controller bool
	nshards    int
	group      string
	ctrlAddrs  []string
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
	f.BoolVar(&cfg.controller, "controller", false, "run as a shard controller node")
	f.IntVar(&cfg.nshards, "nshards", 256, "number of shards")
	f.StringVar(&cfg.group, "group", "", "shard group ID this node belongs to")
	f.StringSliceVar(&cfg.ctrlAddrs, "ctrl-addrs", nil, "controller RPC addresses")

	cmd.MarkFlagRequired("id")

	return cmd
}

func runServe(cfg config) error {
	if cfg.controller {
		return runController(cfg)
	}

	if err := os.MkdirAll(cfg.dataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	s, err := sqlite.New(filepath.Join(cfg.dataDir, "kv.db"))
	if err != nil {
		return fmt.Errorf("init store: %w", err)
	}

	node, err := kvraft.NewNode(s, cfg.id, cfg.raftAddr, cfg.dataDir, cfg.nshards)
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
		if err := node.Advertise(cfg.id, cfg.rpcAddr); err != nil {
			return fmt.Errorf("advertise self: %w", err)
		}
		slog.Info("cluster bootstrapped", "id", cfg.id, "raft", cfg.raftAddr)
	default:
		if cfg.join == "" {
			return fmt.Errorf("a non-bootstrap node needs --join")
		}
		slog.Info("joining cluster", "id", cfg.id, "join", cfg.join)
		if err := joinCluster(cfg); err != nil {
			return fmt.Errorf("join cluster: %w", err)
		}
		slog.Info("successfully joined cluster", "id", cfg.id, "join", cfg.join)
	}

	kv := &server.KVService{Store: node}
	cluster := &server.ClusterService{Node: node}
	migration := &server.MigrationService{Node: node}
	srv := server.New(cfg.rpcAddr, kv, cluster, migration)
	if err := srv.Listen(); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	slog.Info("listening", "rpc", srv.Addr(), "raft", cfg.raftAddr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.group != "" && len(cfg.ctrlAddrs) > 0 {
		ctrlClient := shardctrl.NewClient(cfg.ctrlAddrs)
		mig := migrate.New(node, ctrlClient, cfg.group, cfg.nshards)
		go mig.Run(ctx)
		slog.Info("migrator started", "group", cfg.group, "ctrl", cfg.ctrlAddrs)
	}

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

func runController(cfg config) error {
	if err := os.MkdirAll(cfg.dataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	node, err := shardctrl.NewNode(cfg.nshards, cfg.id, cfg.raftAddr, cfg.dataDir)
	if err != nil {
		return fmt.Errorf("init controller node: %w", err)
	}

	switch {
	case cfg.bootstrap:
		if err := node.Bootstrap(); err != nil {
			return fmt.Errorf("bootstrap controller: %w", err)
		}
		if err := node.WaitForLeader(10 * time.Second); err != nil {
			return fmt.Errorf("wait for leader: %w", err)
		}
		slog.Info("controller bootstrapped", "id", cfg.id, "raft", cfg.raftAddr)
	default:
		if cfg.join == "" {
			return fmt.Errorf("a non-bootstrap controller needs --join")
		}
		slog.Info("joining controller", "id", cfg.id, "join", cfg.join)
		c := shardctrl.NewClient([]string{cfg.join})
		if err := c.AddNode(cfg.id, cfg.raftAddr); err != nil {
			c.Close()
			return fmt.Errorf("join controller: %w", err)
		}
		c.Close()
		slog.Info("joined controller", "id", cfg.id)
	}

	ctrl := &server.ControllerService{Node: node}
	srv := server.New(cfg.rpcAddr, ctrl)
	if err := srv.Listen(); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	slog.Info("controller listening", "rpc", srv.Addr(), "raft", cfg.raftAddr)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	slog.Info("shutting down controller")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("failed to shut down server", "error", err)
	}
	if err := node.Close(); err != nil {
		slog.Error("failed to close controller node", "error", err)
	}
	return nil
}

func joinCluster(cfg config) error {
	const tries = 20
	args := &kvapi.JoinArgs{NodeID: cfg.id, RaftAddr: cfg.raftAddr, RpcAddr: cfg.rpcAddr}

	var lastErr error
	for i := 0; i < tries; i++ {
		client, err := rpc.DialHTTP("tcp", cfg.join)
		if err != nil {
			lastErr = err
			time.Sleep(500 * time.Millisecond)
			continue
		}
		err = client.Call("ClusterService.Join", args, &kvapi.Empty{})
		client.Close()
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return lastErr
}
