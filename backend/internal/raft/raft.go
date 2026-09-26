// Package raft implements a simplified but correct Raft consensus protocol
// for the FT-DOSS metadata service. This ensures metadata consistency across
// multiple metadata nodes and prevents split-brain scenarios.
package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ft-doss/backend/pkg/logger"
	"github.com/ft-doss/backend/pkg/types"
)

// ─── Constants ────────────────────────────────────────────────────────────────

const (
	minElectionTimeoutMS = 150
	maxElectionTimeoutMS = 300
	heartbeatIntervalMS  = 50
	maxLogEntriesPerRPC  = 100
)

// ─── Log Commands ─────────────────────────────────────────────────────────────

// CommandType identifies what a log entry represents.
type CommandType string

const (
	CmdPutObject     CommandType = "PUT_OBJECT"
	CmdDeleteObject  CommandType = "DELETE_OBJECT"
	CmdPutBucket     CommandType = "PUT_BUCKET"
	CmdDeleteBucket  CommandType = "DELETE_BUCKET"
	CmdClusterEpoch  CommandType = "CLUSTER_EPOCH"
	CmdNodeJoin      CommandType = "NODE_JOIN"
	CmdNodeLeave     CommandType = "NODE_LEAVE"
	CmdNoop          CommandType = "NOOP"
)

// LogCommand is the payload of a Raft log entry.
type LogCommand struct {
	Type      CommandType     `json:"type"`
	Sequence  int64           `json:"sequence"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// logEntry is an internal Raft log entry.
type logEntry struct {
	Index   int64
	Term    int64
	Command LogCommand
}

// ─── RPC Types ────────────────────────────────────────────────────────────────

// AppendEntriesRequest is sent by the leader to replicate log entries.
type AppendEntriesRequest struct {
	Term         int64      `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int64      `json:"prev_log_index"`
	PrevLogTerm  int64      `json:"prev_log_term"`
	Entries      []logEntry `json:"entries"`
	LeaderCommit int64      `json:"leader_commit"`
}

// AppendEntriesResponse is the follower reply to AppendEntries.
type AppendEntriesResponse struct {
	Term    int64  `json:"term"`
	Success bool   `json:"success"`
	NodeID  string `json:"node_id"`
	// Optimization: tell leader where to retry from
	ConflictIndex int64 `json:"conflict_index,omitempty"`
	ConflictTerm  int64 `json:"conflict_term,omitempty"`
}

// RequestVoteRequest is sent by a candidate during election.
type RequestVoteRequest struct {
	Term         int64  `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int64  `json:"last_log_index"`
	LastLogTerm  int64  `json:"last_log_term"`
}

// RequestVoteResponse is a peer's reply to a vote request.
type RequestVoteResponse struct {
	Term        int64  `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	NodeID      string `json:"node_id"`
}

// ─── State Machine ────────────────────────────────────────────────────────────

// StateMachine is the interface for applying committed log entries.
type StateMachine interface {
	Apply(entry LogCommand) error
	Snapshot() ([]byte, error)
	Restore(snapshot []byte) error
}

// ─── Peer Transport ───────────────────────────────────────────────────────────

// PeerTransport handles communication with other Raft peers.
type PeerTransport interface {
	AppendEntries(ctx context.Context, peerID string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	RequestVote(ctx context.Context, peerID string, req *RequestVoteRequest) (*RequestVoteResponse, error)
}

// ─── Node ─────────────────────────────────────────────────────────────────────

// Node is a single Raft participant.
type Node struct {
	mu      sync.RWMutex
	log     []logEntry
	nodeID  string
	peers   []string // IDs of other nodes

	// Persistent state (must survive restarts)
	currentTerm int64
	votedFor    string

	// Volatile state
	state       types.RaftState
	commitIndex int64
	lastApplied int64
	leaderID    string

	// Leader-only volatile state
	nextIndex  map[string]int64 // for each peer, next log index to send
	matchIndex map[string]int64 // for each peer, highest confirmed replicated index

	// Infrastructure
	sm        StateMachine
	transport PeerTransport
	logger    *logger.Logger

	// Control
	electionTimer *time.Timer
	heartbeatTick *time.Ticker
	stopCh        chan struct{}
	applyCh       chan struct{}

	// Metrics (atomic)
	electionCount int64
	commitCount   int64

	// Event channel for UI/dashboard
	eventCh chan RaftEvent
}

// RaftEvent is emitted by the Raft node for observability.
type RaftEvent struct {
	Type      string    `json:"type"`
	NodeID    string    `json:"node_id"`
	Term      int64     `json:"term"`
	Index     int64     `json:"index"`
	LeaderID  string    `json:"leader_id,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// NewNode creates a new Raft node.
func NewNode(nodeID string, peers []string, sm StateMachine, transport PeerTransport, log *logger.Logger) *Node {
	n := &Node{
		nodeID:    nodeID,
		peers:     peers,
		sm:        sm,
		transport: transport,
		logger:    log,
		state:     types.RaftStateFollower,
		nextIndex:  make(map[string]int64),
		matchIndex: make(map[string]int64),
		stopCh:    make(chan struct{}),
		applyCh:   make(chan struct{}, 16),
		eventCh:   make(chan RaftEvent, 256),
	}
	return n
}

// Start begins the Raft node's event loop.
func (n *Node) Start() {
	n.resetElectionTimer()
	go n.run()
	go n.applyLoop()
}

// Stop halts the Raft node.
func (n *Node) Stop() {
	close(n.stopCh)
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	if n.heartbeatTick != nil {
		n.heartbeatTick.Stop()
	}
}

// Events returns the event channel for external monitoring.
func (n *Node) Events() <-chan RaftEvent { return n.eventCh }

// IsLeader returns true if this node is the current Raft leader.
func (n *Node) IsLeader() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state == types.RaftStateLeader
}

// LeaderID returns the current known leader ID.
func (n *Node) LeaderID() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderID
}

// Term returns the current Raft term.
func (n *Node) Term() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}

// CommitIndex returns the last committed log index.
func (n *Node) CommitIndex() int64 {
	return atomic.LoadInt64(&n.commitIndex)
}

// LastApplied returns the last applied log index.
func (n *Node) LastApplied() int64 {
	return atomic.LoadInt64(&n.lastApplied)
}

// State returns the current Raft state of this node.
func (n *Node) State() types.RaftState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

// Propose submits a command to the Raft log. Only succeeds on leader.
func (n *Node) Propose(ctx context.Context, cmd LogCommand) (int64, int64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != types.RaftStateLeader {
		return 0, 0, fmt.Errorf("not leader: current leader is %s", n.leaderID)
	}

	cmd.Sequence = n.lastLogIndex() + 1
	cmd.Timestamp = time.Now()

	entry := logEntry{
		Index:   n.lastLogIndex() + 1,
		Term:    n.currentTerm,
		Command: cmd,
	}
	n.log = append(n.log, entry)
	n.logger.RaftEvent("PROPOSE", n.nodeID, n.currentTerm, entry.Index)

	// In single-node mode (no peers), immediately commit and apply the entry.
	// In multi-node mode, trigger replication and wait for quorum.
	if len(n.peers) == 0 {
		atomic.StoreInt64(&n.commitIndex, entry.Index)
		n.mu.Unlock()  // release lock before applying
		n.applyCommitted()
		n.mu.Lock()    // re-acquire for deferred unlock
	} else {
		go n.broadcastAppendEntries()
	}

	return entry.Index, n.currentTerm, nil
}

// WaitForCommit blocks until the given index has been applied to the state machine.
// We wait for lastApplied >= index (not just commitIndex) to ensure the state machine
// has processed the entry before returning to the caller.
func (n *Node) WaitForCommit(ctx context.Context, index int64) error {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()

	// Deadline to avoid infinite wait
	deadline := time.Now().Add(10 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return fmt.Errorf("WaitForCommit timeout: index %d not applied (lastApplied=%d)", index, atomic.LoadInt64(&n.lastApplied))
			}
			if atomic.LoadInt64(&n.lastApplied) >= index {
				return nil
			}
		}
	}
}

// ─── Event Loop ───────────────────────────────────────────────────────────────

func (n *Node) run() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		n.mu.RLock()
		state := n.state
		n.mu.RUnlock()

		switch state {
		case types.RaftStateFollower, types.RaftStateCandidate:
			// Wait for election timer
			select {
			case <-n.stopCh:
				return
			case <-func() <-chan time.Time {
				n.mu.RLock()
				ch := n.electionTimer.C
				n.mu.RUnlock()
				return ch
			}():
				n.startElection()
			}
		case types.RaftStateLeader:
			// Send heartbeats periodically
			select {
			case <-n.stopCh:
				return
			case <-n.heartbeatTick.C:
				n.broadcastAppendEntries()
			}
		}
	}
}

// applyLoop applies committed log entries to the state machine.
func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.applyCh:
			n.applyCommitted()
		}
	}
}

func (n *Node) applyCommitted() {
	commitIdx := atomic.LoadInt64(&n.commitIndex)
	lastApplied := atomic.LoadInt64(&n.lastApplied)

	n.mu.RLock()
	logLen := int64(len(n.log))
	entries := make([]logEntry, 0)
	for i := lastApplied + 1; i <= commitIdx && i <= logLen; i++ {
		entries = append(entries, n.log[i-1]) // log is 0-indexed, i is 1-indexed
	}
	n.mu.RUnlock()

	for _, entry := range entries {
		if n.sm == nil {
			atomic.AddInt64(&n.lastApplied, 1)
			atomic.AddInt64(&n.commitCount, 1)
			continue
		}
		if err := n.sm.Apply(entry.Command); err != nil {
			n.logger.Error().Err(err).
				Int64("index", entry.Index).
				Msg("failed to apply log entry")
		} else {
			atomic.AddInt64(&n.lastApplied, 1)
			atomic.AddInt64(&n.commitCount, 1)
			n.emitEvent("APPLIED", entry.Index)
		}
	}
}

// ─── Election ────────────────────────────────────────────────────────────────

func (n *Node) startElection() {
	n.mu.Lock()
	n.currentTerm++
	n.state = types.RaftStateCandidate
	n.votedFor = n.nodeID
	term := n.currentTerm
	lastIdx := n.lastLogIndex()
	lastTerm := n.lastLogTerm()
	n.mu.Unlock()

	atomic.AddInt64(&n.electionCount, 1)
	n.logger.RaftEvent("ELECTION_START", n.nodeID, term, lastIdx)
	n.emitEvent("ELECTION_START", 0)
	n.resetElectionTimer()

	votes := int32(1) // vote for self
	votesNeeded := (len(n.peers)+2)/2 // majority including self

	var wg sync.WaitGroup
	for _, peer := range n.peers {
		wg.Add(1)
		go func(peerID string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			resp, err := n.transport.RequestVote(ctx, peerID, &RequestVoteRequest{
				Term:         term,
				CandidateID:  n.nodeID,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			})
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if resp.Term > n.currentTerm {
				n.becomeFollower(resp.Term, "")
				return
			}
			if resp.VoteGranted && n.state == types.RaftStateCandidate && n.currentTerm == term {
				if atomic.AddInt32(&votes, 1) >= int32(votesNeeded) {
					n.becomeLeader()
				}
			}
		}(peer)
	}

	// Also check with just our own vote if we have no peers (single-node mode)
	if len(n.peers) == 0 {
		n.mu.Lock()
		n.becomeLeader()
		n.mu.Unlock()
	}
}

func (n *Node) becomeLeader() {
	n.state = types.RaftStateLeader
	n.leaderID = n.nodeID

	// Initialize leader state
	nextIdx := n.lastLogIndex() + 1
	for _, peer := range n.peers {
		n.nextIndex[peer] = nextIdx
		n.matchIndex[peer] = 0
	}

	n.logger.RaftEvent("LEADER_ELECTED", n.nodeID, n.currentTerm, n.lastLogIndex())
	n.emitEvent("LEADER_ELECTED", n.lastLogIndex())

	// Start heartbeat ticker
	if n.heartbeatTick != nil {
		n.heartbeatTick.Stop()
	}
	n.heartbeatTick = time.NewTicker(heartbeatIntervalMS * time.Millisecond)

	// Send immediate heartbeat
	go n.broadcastAppendEntries()

	// Append a noop entry to commit previous terms' entries
	noop := LogCommand{Type: CmdNoop}
	entry := logEntry{
		Index:   n.lastLogIndex() + 1,
		Term:    n.currentTerm,
		Command: noop,
	}
	n.log = append(n.log, entry)
}

func (n *Node) becomeFollower(term int64, leaderID string) {
	wasLeader := n.state == types.RaftStateLeader
	n.state = types.RaftStateFollower
	n.currentTerm = term
	n.votedFor = ""
	if leaderID != "" {
		n.leaderID = leaderID
	}

	if wasLeader && n.heartbeatTick != nil {
		n.heartbeatTick.Stop()
		n.heartbeatTick = nil
	}

	n.resetElectionTimer()
	n.emitEvent("BECAME_FOLLOWER", n.lastLogIndex())
}

// ─── AppendEntries RPC Handler ────────────────────────────────────────────────

// HandleAppendEntries processes an AppendEntries RPC from the leader.
func (n *Node) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &AppendEntriesResponse{
		Term:   n.currentTerm,
		NodeID: n.nodeID,
	}

	// Reject stale term
	if req.Term < n.currentTerm {
		return resp
	}

	// Update term if we're behind
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term, req.LeaderID)
	} else {
		n.leaderID = req.LeaderID
		n.state = types.RaftStateFollower
		n.resetElectionTimer()
	}
	resp.Term = n.currentTerm

	// Check log consistency
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > int64(len(n.log)) {
			resp.ConflictIndex = int64(len(n.log)) + 1
			return resp
		}
		prevEntry := n.log[req.PrevLogIndex-1]
		if prevEntry.Term != req.PrevLogTerm {
			// Find first index of conflicting term
			conflictTerm := prevEntry.Term
			conflictIdx := req.PrevLogIndex
			for conflictIdx > 1 && n.log[conflictIdx-2].Term == conflictTerm {
				conflictIdx--
			}
			resp.ConflictIndex = conflictIdx
			resp.ConflictTerm = conflictTerm
			return resp
		}
	}

	// Append new entries
	for i, entry := range req.Entries {
		idx := req.PrevLogIndex + int64(i) + 1
		if idx <= int64(len(n.log)) {
			if n.log[idx-1].Term != entry.Term {
				// Truncate conflicting entries
				n.log = n.log[:idx-1]
			} else {
				continue // already have this entry
			}
		}
		n.log = append(n.log, entry)
	}

	// Update commit index
	if req.LeaderCommit > n.commitIndex {
		newCommit := req.LeaderCommit
		lastNew := req.PrevLogIndex + int64(len(req.Entries))
		if lastNew < newCommit {
			newCommit = lastNew
		}
		if newCommit > n.commitIndex {
			n.commitIndex = newCommit
			n.signalApply()
		}
	}

	resp.Success = true
	return resp
}

// HandleRequestVote processes a RequestVote RPC from a candidate.
func (n *Node) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &RequestVoteResponse{
		Term:   n.currentTerm,
		NodeID: n.nodeID,
	}

	if req.Term < n.currentTerm {
		return resp
	}

	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term, "")
	}
	resp.Term = n.currentTerm

	// Grant vote if we haven't voted or voted for this candidate
	canVote := n.votedFor == "" || n.votedFor == req.CandidateID
	// Check candidate's log is at least as up-to-date as ours
	upToDate := req.LastLogTerm > n.lastLogTerm() ||
		(req.LastLogTerm == n.lastLogTerm() && req.LastLogIndex >= n.lastLogIndex())

	if canVote && upToDate {
		n.votedFor = req.CandidateID
		resp.VoteGranted = true
		n.resetElectionTimer()
		n.logger.RaftEvent("VOTE_GRANTED", n.nodeID, n.currentTerm, 0)
	}

	return resp
}

// ─── Leader Replication ───────────────────────────────────────────────────────

func (n *Node) broadcastAppendEntries() {
	n.mu.RLock()
	if n.state != types.RaftStateLeader {
		n.mu.RUnlock()
		return
	}
	term := n.currentTerm
	commitIndex := n.commitIndex
	n.mu.RUnlock()

	for _, peer := range n.peers {
		go n.sendAppendEntries(peer, term, commitIndex)
	}
}

func (n *Node) sendAppendEntries(peerID string, term, commitIndex int64) {
	n.mu.RLock()
	nextIdx := n.nextIndex[peerID]
	prevLogIndex := nextIdx - 1
	var prevLogTerm int64
	if prevLogIndex > 0 && prevLogIndex <= int64(len(n.log)) {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}
	var entries []logEntry
	if nextIdx <= int64(len(n.log)) {
		end := nextIdx - 1 + maxLogEntriesPerRPC
		if end > int64(len(n.log)) {
			end = int64(len(n.log))
		}
		entries = n.log[nextIdx-1 : end]
	}
	n.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	resp, err := n.transport.AppendEntries(ctx, peerID, &AppendEntriesRequest{
		Term:         term,
		LeaderID:     n.nodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	})
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term, "")
		return
	}

	if n.state != types.RaftStateLeader || n.currentTerm != term {
		return
	}

	if resp.Success {
		newMatchIndex := prevLogIndex + int64(len(entries))
		if newMatchIndex > n.matchIndex[peerID] {
			n.matchIndex[peerID] = newMatchIndex
		}
		n.nextIndex[peerID] = n.matchIndex[peerID] + 1
		n.maybeAdvanceCommitIndex()
	} else {
		// Back off
		if resp.ConflictIndex > 0 {
			n.nextIndex[peerID] = resp.ConflictIndex
		} else if n.nextIndex[peerID] > 1 {
			n.nextIndex[peerID]--
		}
	}
}

// maybeAdvanceCommitIndex advances the commit index if a majority has replicated.
func (n *Node) maybeAdvanceCommitIndex() {
	for idx := int64(len(n.log)); idx > n.commitIndex; idx-- {
		if n.log[idx-1].Term != n.currentTerm {
			continue // Only commit entries from current term
		}
		count := 1 // count self
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		majority := (len(n.peers)+2)/2
		if count >= majority {
			n.commitIndex = idx
			n.signalApply()
			n.emitEvent("COMMITTED", idx)
			break
		}
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func (n *Node) lastLogIndex() int64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

func (n *Node) lastLogTerm() int64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Term
}

func (n *Node) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	timeout := time.Duration(minElectionTimeoutMS+rand.Intn(maxElectionTimeoutMS-minElectionTimeoutMS)) * time.Millisecond
	n.electionTimer = time.NewTimer(timeout)
}

func (n *Node) signalApply() {
	select {
	case n.applyCh <- struct{}{}:
	default:
	}
}

func (n *Node) emitEvent(eventType string, index int64) {
	event := RaftEvent{
		Type:      eventType,
		NodeID:    n.nodeID,
		Term:      n.currentTerm,
		Index:     index,
		LeaderID:  n.leaderID,
		Timestamp: time.Now(),
	}
	select {
	case n.eventCh <- event:
	default:
	}
}

// GetStatus returns a status snapshot for the dashboard.
func (n *Node) GetStatus() map[string]interface{} {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return map[string]interface{}{
		"node_id":       n.nodeID,
		"state":         string(n.state),
		"term":          n.currentTerm,
		"leader_id":     n.leaderID,
		"commit_index":  n.commitIndex,
		"last_applied":  atomic.LoadInt64(&n.lastApplied),
		"log_length":    len(n.log),
		"election_count": atomic.LoadInt64(&n.electionCount),
		"commit_count":  atomic.LoadInt64(&n.commitCount),
	}
}
