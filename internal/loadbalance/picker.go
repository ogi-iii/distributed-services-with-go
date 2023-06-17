package loadbalance

import (
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

type Picker struct {
	mu        sync.RWMutex
	leader    balancer.SubConn   // a connection to the leader in the cluster
	followers []balancer.SubConn // some connections to the followers in the cluster
	current   uint64
}

func init() {
	balancer.Register(
		base.NewBalancerBuilder(Name, &Picker{}, base.Config{})) // register as gRPC server connection picker
}

// checking Picker implements the interface: base.PickerBuilder
var _ base.PickerBuilder = (*Picker)(nil)

func (p *Picker) Build(buildInfo base.PickerBuildInfo) balancer.Picker {
	p.mu.Lock()
	defer p.mu.Unlock()
	var followers []balancer.SubConn
	for sc, scInfo := range buildInfo.ReadySCs { // loop the sub-connections from the resolver and divide as leader or followers
		isLeader := scInfo.Address.Attributes.Value("is_leader").(bool)
		if isLeader {
			p.leader = sc
			continue
		}
		followers = append(followers, sc)
	}
	p.followers = followers
	return p
}

// checking Picker implements the interface: balancer.Picker
var _ balancer.Picker = (*Picker)(nil)

func (p *Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.RLock() // lock as read-only mode
	defer p.mu.RUnlock()
	var result balancer.PickResult
	// send the produce request to leader or single node
	if strings.Contains(info.FullMethodName, "Produce") || len(p.followers) == 0 {
		result.SubConn = p.leader
		// balance the consume request if the followers exists
	} else if strings.Contains(info.FullMethodName, "Consume") {
		result.SubConn = p.nextFollower() // pick up a followers
	}
	if result.SubConn == nil {
		return result, balancer.ErrNoSubConnAvailable
	}
	return result, nil
}

func (p *Picker) nextFollower() balancer.SubConn {
	cur := atomic.AddUint64(&p.current, uint64(1))
	len := uint64(len(p.followers))
	idx := int(cur % len) // balance the connections with the number of followers: round-robin algorithm
	return p.followers[idx]
}
