package agent

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"

	api "github.com/ogi-iii/proglog/api/v1"
	"github.com/ogi-iii/proglog/internal/config"
	"github.com/ogi-iii/proglog/internal/loadbalance"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc/credentials"
)

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.ServerCertFile,
			KeyFile:       config.ServerKeyFile,
			CAFile:        config.CAFile,
			Server:        true,
			ServerAddress: "127.0.0.1",
		},
	)
	require.NoError(t, err)
	peerTLSConfig, err := config.SetupTLSConfig(
		config.TLSConfig{
			CertFile:      config.RootClientCertFile,
			KeyFile:       config.RootClientKeyFile,
			CAFile:        config.CAFile,
			Server:        false,
			ServerAddress: "127.0.0.1",
		},
	)
	require.NoError(t, err)
	var agents []*Agent
	for i := 0; i < 3; i++ {
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]
		dataDir, err := ioutil.TempDir("", "agent-test-log")
		require.NoError(t, err)
		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr) // join the cluster of the first node
		}
		agent, err := New(Config{
			NodeName:        fmt.Sprintf("%d", i),
			StartJoinAddrs:  startJoinAddrs,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
			Bootstrap:       i == 0,
		})
		require.NoError(t, err)
		agents = append(agents, agent)
	}
	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(agent.Config.DataDir))
		}
	}()
	time.Sleep(3 * time.Second) // for waiting the service discovery of each node

	leaderClient := client(t, agents[0], peerTLSConfig)
	// produce a record to leader node
	produceResponse, err := leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("Hello, world!"),
			},
		},
	)
	require.NoError(t, err)

	time.Sleep(3 * time.Second) // for waiting the replication from the reader to followers

	// try to consume a record from the leader: the consume request will be balanced to the other followers instead of the leader by our picker
	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello, world!"), consumeResponse.Record.Value)

	followerClient := client(t, agents[1], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, []byte("Hello, world!"), consumeResponse.Record.Value)

	// check the Raft replicator coordinates the servers as leader-follower relationship: the leader does NOT replicate from the followers
	consumeResponse, err = leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset + 1, // not produced record
		},
	)
	require.Nil(t, consumeResponse)
	require.Error(t, err)
	got := grpc.Code(err)
	want := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, got, want)
}

// generate the gRPC client to connect an agent: service discovery with resolver, load balancing with picker
func client(
	t *testing.T,
	agent *Agent,
	tlsConfig *tls.Config,
) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(tlsCreds),
	}
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)
	conn, err := grpc.Dial(
		fmt.Sprintf(
			"%s:///%s",
			loadbalance.Name, // setup the scheme of our resolver for service discovery: the gRPC requests will be balanced by our picker
			rpcAddr,
		),
		opts...,
	)
	// conn, err := grpc.Dial(fmt.Sprintf("%s", rpcAddr), opts...)
	require.NoError(t, err)
	client := api.NewLogClient(conn)
	return client
}
