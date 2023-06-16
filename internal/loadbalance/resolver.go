package loadbalance

import (
	"context"
	"fmt"
	"sync"

	api "github.com/ogi-iii/proglog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

type Resolver struct {
	mu            sync.Mutex
	clientConn    resolver.ClientConn // resolver -> user's client: tell the state which is the result of service discovery for load-balancing
	resolverConn  *grpc.ClientConn    // resolver -> a gRPC server in the cluster: service discovery
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

var _ resolver.Builder = (*Resolver)(nil)

func (r *Resolver) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions, // options to connect a server in the cluster
) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc // connection to the user's client
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(
			dialOpts,
			grpc.WithTransportCredentials(opts.DialCreds),
		)
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig( // gRPC load-balancing service config
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, Name),
	)
	var err error
	r.resolverConn, err = grpc.Dial(target.Endpoint(), dialOpts...) // connect to the servers
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{}) // initial service discovery
	return r, nil
}

const Name = "proglog"

func (r *Resolver) Scheme() string {
	return Name // scheme identifier to find a resolver that matches
}

func init() {
	resolver.Register(&Resolver{}) // register as gRPC resolver
}

var _ resolver.Resolver = (*Resolver)(nil)

func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	client := api.NewLogClient(r.resolverConn)
	// get cluster and then set on cc attributes
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{}) // call to get the servers in the cluster
	if err != nil {
		r.logger.Error(
			"failed to resolve server",
			zap.Error(err),
		)
		return
	}
	var addrs []resolver.Address
	for _, server := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr: server.RpcAddr,
			Attributes: attributes.New(
				"is_leader",
				server.IsLeader,
			),
		})
	}
	r.clientConn.UpdateState(resolver.State{ // update the state of service discovery for load-balancing
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

func (r *Resolver) Close() {
	// close to stop service discovery
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error(
			"failed to close conn",
			zap.Error(err),
		)
	}
}
