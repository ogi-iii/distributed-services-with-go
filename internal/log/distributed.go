package log

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	raftboltdb "github.com/hashicorp/raft-boltdb"
	api "github.com/ogi-iii/proglog/api/v1"
)

type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	l := &DistributedLog{
		config: config,
	}
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	var err error
	l.log, err = NewLog(logDir, l.config)
	return err
}

func (l *DistributedLog) setupRaft(dataDir string) error {
	// finite-state machine that applies commands
	fsm := &fsm{
		log: l.log,
	}
	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	logConfig := l.config
	logConfig.Segment.InitialOffset = 1
	// log store where Raft stores the history of commands
	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}
	// stable store where Raft stores the configurations of the cluster
	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(dataDir, "raft", "stable"),
	)
	if err != nil {
		return err
	}
	retain := 1
	// snapshot store where Raft stores compact the leader's snapshots of commands data: to restore the data efficiently
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return err
	}
	maxPool := 5
	timeout := 10 * time.Second
	// transport that Raft uses to connect with the server's peers
	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)
	config := raft.DefaultConfig()
	config.LocalID = l.config.Raft.LocalID
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}
	l.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}
	hasState, err := raft.HasExistingState(
		logStore,
		stableStore,
		snapshotStore,
	)
	if err != nil {
		return err
	}
	// startup a cluster as initial node: subsequently added nodes do not bootstrap a cluster
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      config.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}
	return err
}

func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(
		AppendRequestType,
		&api.ProduceRequest{
			Record: record,
		},
	)
	if err != nil {
		return 0, err
	}
	return res.(*api.ProduceResponse).Offset, nil
}

func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (
	interface{},
	error,
) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)}) // 1 byte data
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(req) // protobuf -> bytes
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(b) // RequestType + data
	if err != nil {
		return nil, err
	}
	timeout := 10 * time.Second
	// append log with Raft replication
	future := l.raft.Apply(buf.Bytes(), timeout) // call fsm.Apply() internally
	// Raft replication error handling
	if future.Error() != nil {
		return nil, future.Error()
	}
	res := future.Response()
	// fsm.Apply() internal error handling
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset) // read local log data directly
}

// finite-state machine of Raft
var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log
}

type RequestType uint8 // 1 byte

// multiple kinds of RequestType
const (
	AppendRequestType RequestType = 0
)

// apply committed Raft log (command history)
func (f *fsm) Apply(record *raft.Log) interface{} { // return struct or error
	buf := record.Data
	reqType := RequestType(buf[0]) // extract first 1 byte
	switch reqType {
	case AppendRequestType:
		return f.applyAppend(buf[1:])
	}
	return nil
}

func (f *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req) // bytes -> protobuf
	if err != nil {
		return err
	}
	offset, err := f.log.Append(req.Record) // append a record to local log
	if err != nil {
		return err
	}
	return &api.ProduceResponse{Offset: offset}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

// save the snapshot to sink
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	// copy from reader data to sink disc
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

// delete the snapshot from sink disc
func (s *snapshot) Release() {}

// restore data from the newest snapshot of leader node
func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth) // data length
	var buf bytes.Buffer        // data itself
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b) // read data until the size of b
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		size := int64(enc.Uint64(b))
		// copy data from 'r' to 'buf' until the length of 'size'
		if _, err := io.CopyN(&buf, r, size); err != nil {
			return err
		}
		record := &api.Record{}
		// bytes -> protobuf
		if err := proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}
		// initial data handling
		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil { // reset existing data in the local log
				return err
			}
		}
		if _, err := f.log.Append(record); err != nil {
			return err
		}
		buf.Reset() // reset buffered data to prepare reading the next data
	}
	return nil
}

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, err // wrap the record log as logStore
}

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	off, err := l.HighestOffset()
	return off, err
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error { // pass the input offset & the output blank Raft log
	in, err := l.Read(index)
	if err != nil {
		return err
	}
	// set input data as output Raft log data
	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term
	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		// store as a Raft log
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max) // delete up to max offsets
}
