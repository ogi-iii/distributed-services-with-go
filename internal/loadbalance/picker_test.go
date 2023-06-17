package loadbalance

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

var (
	produceMethod string = "/log.vX.Log/Produce"
	consumeMethod string = "/log.vX.Log/Consume"
)

func TestPickerNoSubConnAvailable(t *testing.T) {
	picker := &Picker{}
	for _, method := range []string{
		produceMethod,
		consumeMethod,
	} {
		info := balancer.PickInfo{
			FullMethodName: method,
		}
		// receive the request method but there is no sub-connections that can handle the request
		result, err := picker.Pick(info)
		require.Equal(t, balancer.ErrNoSubConnAvailable, err)
		require.Nil(t, result.SubConn)
	}
}

func TestPickerProduceToLeader(t *testing.T) {
	picker, subConns := setupTest()
	info := balancer.PickInfo{
		FullMethodName: produceMethod,
	}
	for i := 0; i < 5; i++ {
		// receive produce request and all of the requests will be sent to the leader
		gotPick, err := picker.Pick(info)
		require.NoError(t, err)
		require.Equal(t, subConns[0], gotPick.SubConn)
	}
}

func TestPickerConsumeFromFollowers(t *testing.T) {
	picker, subConns := setupTest()
	info := balancer.PickInfo{
		FullMethodName: consumeMethod,
	}
	for i := 0; i < 5; i++ {
		// receive consume request and each request will be balanced to the followers by round-robin
		pick, err := picker.Pick(info)
		require.NoError(t, err)
		require.Equal(t, subConns[i%2+1], pick.SubConn) // balance with a follower
	}
}

func setupTest() (*Picker, []*subConn) {
	var subConns []*subConn
	buildInfo := base.PickerBuildInfo{
		ReadySCs: make(map[balancer.SubConn]base.SubConnInfo),
	}
	for i := 0; i < 3; i++ {
		sc := &subConn{} // mock
		addr := resolver.Address{
			Attributes: attributes.New("is_leader", i == 0),
		}
		// 0th sub-connection is the leader
		sc.UpdateAddresses([]resolver.Address{addr})
		buildInfo.ReadySCs[sc] = base.SubConnInfo{Address: addr}
		subConns = append(subConns, sc)
	}
	picker := &Picker{}
	picker.Build(buildInfo)
	return picker, subConns
}

// mock
type subConn struct {
	addrs []resolver.Address
}

func (s *subConn) UpdateAddresses(addrs []resolver.Address) {
	s.addrs = addrs
}

func (s *subConn) Connect() {}

func (s *subConn) GetOrBuildProducer(produceBuilder balancer.ProducerBuilder) (p balancer.Producer, close func()) {
	return nil, nil
}
