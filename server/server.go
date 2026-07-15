package server

import (
	"context"
	"net"
	"net/http"
	"net/rpc"

	"github.com/fredouric/kvraft/kvapi"
	"github.com/fredouric/kvraft/store"
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

func (k *KVService) Set(args *kvapi.SetArgs, reply *kvapi.Empty) error {
	return k.Store.Set(args.Key, args.Value)
}

func (k *KVService) Delete(args *kvapi.DeleteArgs, reply *kvapi.Empty) error {
	return k.Store.Delete(args.Key)
}

type Server struct {
	addr string
	ln   net.Listener

	kv   *KVService
	http *http.Server
}

func New(addr string, kv *KVService) *Server {
	return &Server{
		addr: addr,
		kv:   kv,
	}
}

func (s *Server) Addr() string {
	return s.ln.Addr().String()
}

func (s *Server) Listen() error {

	server := rpc.NewServer()
	err := server.Register(s.kv)
	if err != nil {
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
