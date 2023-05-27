package server

import (
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "github.com/ogi-iii/proglog/api/v1"
	"github.com/ogi-iii/proglog/internal/config"
	"github.com/ogi-iii/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var testScenarios = map[string]func(
	t *testing.T,
	rootClient api.LogClient,
	nobodyClient api.LogClient,
	config *Config,
){
	"produce/consume a message to/from the log scceeds": testProduceConsume,
	"produce/consume stream succeeds":                   testProduceConsumeStream,
	"consume past log boundary fails":                   testConsumePastBoundary,
}

func TestServer(t *testing.T) {
	for scenario, fn := range testScenarios {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	rootClient api.LogClient,
	nobodyClient api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()
	// setup endpoint
	l, err := net.Listen("tcp", "127.0.0.1:0")
	// l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	// setup client
	newClient := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		api.LogClient,
		[]grpc.DialOption,
	) {
		tlsConfig, err := config.SetupTLSConfig(
			config.TLSConfig{
				CertFile: crtPath,
				KeyFile:  keyPath,
				CAFile:   config.CAFile,
				Server:   false,
			})
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(tlsCreds),
		}
		conn, err := grpc.Dial(l.Addr().String(), opts...)
		require.NoError(t, err)
		client := api.NewLogClient(conn)
		return conn, client, opts
	}
	// clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
	// 	CertFile: config.ClientCertFile,
	// 	KeyFile:  config.ClientKeyFile,
	// 	CAFile:   config.CAFile,
	// 	Server:   false,
	// })
	// require.NoError(t, err)
	// clientCreds := credentials.NewTLS(clientTLSConfig)
	// cc, err := grpc.Dial(
	// 	l.Addr().String(),
	// 	grpc.WithTransportCredentials(clientCreds),
	// )
	// // clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	// // cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	// require.NoError(t, err)
	// client = api.NewLogClient(cc)
	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)
	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

	// setup server
	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)
	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)
	cfg = &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(cfg)
	}
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	// server, err := NewGRPCServer(*cfg)
	require.NoError(t, err)
	go func() {
		server.Serve(l)
	}()
	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		// cc.Close()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		clog.Remove()
	}
}

func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()
	want := &api.Record{
		Value: []byte("Hello, world!"),
	}
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)
	consume, err := client.Consume(
		ctx,
		&api.ConsumeRequest{
			Offset: produce.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("Hello, world!"),
			},
		},
	)
	require.NoError(t, err)
	consume, err := client.Consume(
		ctx,
		&api.ConsumeRequest{
			Offset: produce.Offset + 1,
		},
	)
	if consume != nil {
		t.Fatal("consume not nil")
	}
	got := grpc.Code(err)
	want := grpc.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()
	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)
		for offset, record := range records {
			err = stream.Send(
				&api.ProduceRequest{
					Record: record,
				},
			)
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d", res.Offset, offset)
			}
		}
	}
	{
		stream, err := client.ConsumeStream(
			ctx,
			&api.ConsumeRequest{
				Offset: 0,
			},
		)
		require.NoError(t, err)
		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(
				t,
				res.Record,
				&api.Record{
					Value:  record.Value,
					Offset: uint64(i),
				},
			)
		}
	}
}
