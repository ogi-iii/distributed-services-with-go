package log

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"

	api "github.com/ogi-iii/proglog/api/v1"
)

func TestMultipleNodes(t *testing.T) {
	var logs []*DistributedLog
	nodeCount := 3
	ports := dynaport.Get(nodeCount)
	for i := 0; i < nodeCount; i++ {
		dataDir, err := ioutil.TempDir("", "distributed-log-test")
		require.NoError(t, err)
		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dataDir)
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[i]))
		require.NoError(t, err)
		config := Config{}
		config.Raft.StreamLayer = NewStreamLayer(ln, nil, nil)
		config.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", i))
		// shorten Raft timeout settings for testing
		config.Raft.HeartbeatTimeout = 50 * time.Millisecond
		config.Raft.ElectionTimeout = 50 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
		config.Raft.CommitTimeout = 5 * time.Millisecond
		config.Raft.BindAddr = ln.Addr().String() // advertise the Raft address
		if i == 0 {
			config.Raft.Bootstrap = true
		}
		l, err := NewDistributedLog(dataDir, config)
		require.NoError(t, err)
		if i != 0 {
			// join the existing Raft cluster
			err = logs[0].Join(
				fmt.Sprintf("%d", i),
				ln.Addr().String(),
			)
			require.NoError(t, err)
		} else {
			// wait for the Raft leader election
			err = l.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
		}
		logs = append(logs, l)
	}
	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}
	for _, record := range records {
		// append logs to the Raft leader node
		off, err := logs[0].Append(record)
		require.NoError(t, err)
		// check if the Raft leader node replicates to follower nodes
		require.Eventually(t, func() bool {
			for j := 0; j < nodeCount; j++ {
				got, err := logs[j].Read(off)
				if err != nil {
					return false
				}
				record.Offset = off
				if !reflect.DeepEqual(got.Value, record.Value) {
					return false
				}
			}
			return true
		}, 500*time.Millisecond, 50*time.Millisecond) // repeat checking
	}

	// check to get servers in the Raft cluster
	servers, err := logs[0].GetServers()
	require.NoError(t, err)
	require.Equal(t, 3, len(servers))
	require.True(t, servers[0].IsLeader)
	require.False(t, servers[1].IsLeader)
	require.False(t, servers[2].IsLeader)

	// remove the Raft follower node from the cluster
	err = logs[0].Leave("1")
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// check to get servers again
	servers, err = logs[0].GetServers()
	require.NoError(t, err)
	require.Equal(t, 2, len(servers)) // a follower was left from the Raft cluster
	require.True(t, servers[0].IsLeader)
	require.False(t, servers[1].IsLeader)

	// append an additional record to the Raft cluster
	off, err := logs[0].Append(&api.Record{
		Value: []byte("third"),
	})
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	// check that the log is NOT found on the removed node
	record, err := logs[1].Read(off)
	require.IsType(t, api.ErrOffsetOutOfRange{}, err)
	require.Nil(t, record)
	// check that the replicated log is found on the existing Raft follower node
	record, err = logs[2].Read(off)
	require.NoError(t, err)
	require.Equal(t, []byte("third"), record.Value)
	require.Equal(t, off, record.Offset)
}
