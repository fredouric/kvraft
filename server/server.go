package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/rpc"

	"github.com/fredouric/kvraft/kvapi"
	"github.com/fredouric/kvraft/store"
	"github.com/fredouric/kvraft/store/kvraft"
)

type KVService struct {
	Store store.Store
}

func (k *KVService) Get(args *kvapi.GetArgs, reply *kvapi.GetReply) error {
	v, ok, err := k.Store.Get(args.Key)
	if err != nil {
		return err
	}
	reply.Value = v
	reply.Found = ok
	return nil
}

func (k *KVService) Set(args *kvapi.SetArgs, reply *kvapi.WriteReply) error {
	return redirectOrError(k.Store.Set(args.Key, args.Value), reply)
}

func (k *KVService) Delete(args *kvapi.DeleteArgs, reply *kvapi.WriteReply) error {
	return redirectOrError(k.Store.Delete(args.Key), reply)
}

func redirectOrError(err error, reply *kvapi.WriteReply) error {
	var nle *kvraft.NotLeaderError
	if errors.As(err, &nle) {
		reply.NotLeader = true
		reply.LeaderAddr = nle.LeaderAddr
		return nil
	}
	return err
}

type Server struct {
	addr string
	ln   net.Listener

	kv      *KVService
	http    *http.Server
	cluster *ClusterService
}

func New(addr string, kv *KVService, cluster *ClusterService) *Server {
	return &Server{
		addr:    addr,
		kv:      kv,
		cluster: cluster,
	}
}

func (s *Server) Addr() string {
	return s.ln.Addr().String()
}

func (s *Server) Listen() error {

	server := rpc.NewServer()
	if err := server.Register(s.kv); err != nil {
		return err
	}
	if err := server.Register(s.cluster); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, server)

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln
	s.http = &http.Server{Handler: mux}

	go s.http.Serve(ln)
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}
