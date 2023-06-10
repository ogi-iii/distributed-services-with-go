package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/hashicorp/raft"
	"github.com/ogi-iii/proglog/internal/auth"
	"github.com/ogi-iii/proglog/internal/discovery"
	"github.com/ogi-iii/proglog/internal/log"
	"github.com/ogi-iii/proglog/internal/server"
	"github.com/soheilhy/cmux"
)

type Agent struct {
	Config

	mux cmux.CMux // connection multiplexer

	log        *log.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

type Config struct {
	ServerTLSConfig *tls.Config // local grpc server config
	PeerTLSConfig   *tls.Config // grpc client config connecting to other servers
	DataDir         string
	BindAddr        string // for service discovery with Serf
	RPCPort         int    // for replicating logs with gRPC
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
	Bootstrap       bool // for starting the Raft cluster
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr) // split addr to host & port
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}
	setup := []func() error{
		a.setupLogger,
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	// start running the server
	go a.serve()
	// stop the server
	return a, nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment() // DebugLevel and above logs to standard error in a human-friendly format
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

func (a *Agent) setupMux() error {
	rpcAddr := fmt.Sprintf(
		":%d",
		a.Config.RPCPort,
	)
	ln, err := net.Listen("tcp", rpcAddr) // setup the server listener address (IP:Port)
	if err != nil {
		return err
	}
	a.mux = cmux.New(ln) // accept both Raft and gRPC connections
	return nil
}

func (a *Agent) setupLog() error {
	raftLn := a.mux.Match(func(reader io.Reader) bool { // match connections based on the configured rules
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil { // read first one byte
			return false
		}
		return bytes.Compare(b, []byte{byte(log.RaftRPC)}) == 0 // check if the value matches our Raft identifier
	})
	logConfig := log.Config{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftLn, // multiplexed listener with configured rules
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
	)
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap
	var err error
	a.log, err = log.NewDistributedLog(
		a.Config.DataDir,
		logConfig, // configs for Log & Raft
	)
	if err != nil {
		return err
	}
	if a.Config.Bootstrap {
		err = a.log.WaitForLeader(3 * time.Second)
	}
	return err
}

func (a *Agent) setupServer() error {
	// generate ACL
	authorizer := auth.New(
		a.Config.ACLModelFile,
		a.Config.ACLPolicyFile,
	)
	serverConfig := &server.Config{
		CommitLog:  a.log,
		Authorizer: authorizer,
	}
	var opts []grpc.ServerOption
	// pass the CA file as TLS credentials
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	var err error
	// generate server
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}
	grpcLn := a.mux.Match(cmux.Any()) // accept any other connections for RPC Port (except for Raft matcher)
	go func() {
		// run the local server with multiplexed listener (the endpoint address has been already setup)
		if err := a.server.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}
	}()
	return err
}

func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	// run the Serf service discovery
	a.membership, err = discovery.New(
		a.log, // Raft defines the Serf handlers: Join(), Leave()
		discovery.Config{
			NodeName: a.Config.NodeName,
			BindAddr: a.Config.BindAddr,
			Tags: map[string]string{
				"rpc_addr": rpcAddr,
			},
			StartJoinAddrs: a.Config.StartJoinAddrs,
		},
	)
	return err
}

func (a *Agent) serve() error {
	// start running the multiplexed server: Raft & gRPC
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}
	return nil
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)
	shutdown := []func() error{
		a.membership.Leave, // send events to other servers
		func() error {
			a.server.GracefulStop() // shutdown the local server
			return nil
		},
		a.log.Close, // close log files
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}
