package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.


import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	Follower = "follower"
	Candidate = "candidate"
	Leader = "leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int
	applyCh   chan raftapi.ApplyMsg 

	// Persistent state 
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state, all servers
	commitIndex int
	lastApplied int
	state       string
	electionDeadline time.Time // when the current election timeout expires

	// Volatile state, leader only 
	nextIndex  []int
	matchIndex []int

	// Persistent, snapshot
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshotPending   bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	rf.persistWithSnapshot(rf.persister.ReadSnapshot())
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	rf.persister.Save(w.Bytes(), snapshot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		panic("readPersist: failed to decode saved state")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.lastIncludedIndex+len(rf.log) {
		return
	}
	term := rf.termAt(index) 
	rf.log = append([]LogEntry(nil), rf.log[index-rf.lastIncludedIndex:]...)
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	rf.persistWithSnapshot(snapshot)
}

func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	last := rf.lastIncludedIndex + len(rf.log)
	return last, rf.termAt(last)
}

// caller must hold rf.mu
// term of the entry at a Raft index in [lastIncludedIndex, last index]
func (rf *Raft) termAt(index int) int {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.log[index-rf.lastIncludedIndex-1].Term
}

func randomElectionTimeout() (time.Duration) {
	return time.Duration(300 + (rand.Int63() % 300)) * time.Millisecond
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	CandidateId int
	Term int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	VoteGranted bool
	Term int
}

type AppendEntriesArgs struct {
	LeaderId int
	Term int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictIndex int
	ConflictTerm int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term{
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	reply.Term = rf.currentTerm
	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
	logUpToDate := args.LastLogTerm > lastLogTerm || 
	(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logUpToDate{
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.electionDeadline = time.Now().Add(randomElectionTimeout())
	} else{
		reply.VoteGranted = false
	}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = Follower
	rf.electionDeadline = time.Now().Add(randomElectionTimeout())
	reply.Term = rf.currentTerm

	prevLogIndex := args.PrevLogIndex
	prevLogTerm := args.PrevLogTerm
	entries := args.Entries

	// the leader is probing below our snapshot: those entries are committed and
	// already inside the snapshot, so skip past them
	if prevLogIndex < rf.lastIncludedIndex {
		skip := rf.lastIncludedIndex - prevLogIndex
		if skip >= len(entries) {
			reply.Success = true
			return
		}
		entries = entries[skip:]
		prevLogIndex = rf.lastIncludedIndex
		prevLogTerm = rf.lastIncludedTerm
	}

	lastLogIndex, _ := rf.lastLogIndexAndTerm()

	if prevLogIndex > lastLogIndex {
		reply.Success = false
		reply.ConflictIndex = lastLogIndex + 1
		reply.ConflictTerm = 0
		return
	}

	prevEntryTerm := rf.termAt(prevLogIndex)
	if prevEntryTerm != prevLogTerm {
		reply.Success = false
		reply.ConflictTerm = prevEntryTerm
		firstIndexOfTerm := prevLogIndex
		for firstIndexOfTerm > rf.lastIncludedIndex+1 && rf.termAt(firstIndexOfTerm-1) == prevEntryTerm {
			firstIndexOfTerm--
		}
		reply.ConflictIndex = firstIndexOfTerm
		return
	}

	for i, entry := range entries {
		entryIndex := prevLogIndex + 1 + i
		if entryIndex <= lastLogIndex {
			if rf.termAt(entryIndex) != entry.Term {
				rf.log = rf.log[:entryIndex-rf.lastIncludedIndex-1]
				rf.log = append(rf.log, entries[i:]...)
				break
			}
		} else {
			rf.log = append(rf.log, entries[i:]...)
			break
		}
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		newLastIndex, _ := rf.lastLogIndexAndTerm()
		rf.commitIndex = min(args.LeaderCommit, newLastIndex)
	}

	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}

	rf.state = Follower
	rf.electionDeadline =  time.Now().Add(randomElectionTimeout())
	reply.Term = rf.currentTerm

	idx := args.LastIncludedIndex
	if idx <= rf.lastIncludedIndex {
		return
	}

	if idx <= rf.lastIncludedIndex + len(rf.log) && rf.termAt(idx) == args.LastIncludedTerm {
		rf.log = append([]LogEntry(nil), rf.log[idx-rf.lastIncludedIndex:]...)
	} else {
		rf.log = make([]LogEntry, 0)
	}

	rf.lastIncludedIndex = idx
	rf.lastIncludedTerm = args.LastIncludedTerm

	if rf.commitIndex < idx {
		rf.commitIndex = idx
	}
	if rf.lastApplied < idx {
		rf.lastApplied = idx
		rf.snapshotPending = true // the applier delivers it to the service, in order
	}
	rf.persistWithSnapshot(args.Data)
	
}
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot (server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return index, term, false
	}

	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.persist()

	index = rf.lastIncludedIndex + len(rf.log)	
	term = rf.currentTerm

	for peer:= range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.replicateTo(peer, term)
	}


	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.state != Leader && time.Now().After(rf.electionDeadline) {
			rf.startElectionLocked()
		}
		rf.mu.Unlock()
		// pause for 20 ms
		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) becomeLeaderLocked() {
	rf.state = Leader
	lastLogIndex, _ := rf.lastLogIndexAndTerm()
	for i := range rf.peers {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	go rf.heartbeatLoop(rf.currentTerm)
}

func (rf *Raft) heartbeatLoop(term int) {
	for true {
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			} 
			go rf.replicateTo(peer, term)
			}
			rf.mu.Unlock()

			time.Sleep(100*time.Millisecond)
		}
	}


func (rf *Raft) startElectionLocked() {
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.electionDeadline = time.Now().Add(randomElectionTimeout())
	rf.persist()

	term := rf.currentTerm
	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
	votes := 1

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			args := &RequestVoteArgs{
				Term: term,
				CandidateId: rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm: lastLogTerm,
			}
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(peer, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.state = Follower
				rf.votedFor = -1
				rf.currentTerm = reply.Term
				rf.persist()
				return
			}

			if rf.state != Candidate || rf.currentTerm != term {
				return
			}

			if reply.VoteGranted{
				votes++
				if votes > len(rf.peers)/2{
					rf.becomeLeaderLocked()
				}
			}

		}(peer)
	}
}

func (rf *Raft) advanceCommitIndexLocked() {
	lastIndex, _ := rf.lastLogIndexAndTerm()
	
	for n:= lastIndex; n > rf.commitIndex; n-- {
		if rf.termAt(n) != rf.currentTerm {
			break
		}
		count:=1
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= n {
				count++
			}
		}
		if count > len(rf.peers) / 2 {
			rf.commitIndex = n
			break
		}
	}
}
func (rf *Raft) replicateTo(peer int, term int) {
	rf.mu.Lock()
	if rf.currentTerm != term || rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm := rf.termAt(prevLogIndex)

	entries := make([]LogEntry, rf.lastIncludedIndex+len(rf.log)-prevLogIndex)
	copy(entries, rf.log[prevLogIndex-rf.lastIncludedIndex:])

	args := &AppendEntriesArgs{
		Term: term,
		LeaderId: rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
		Entries: entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}

	if !rf.sendAppendEntries(peer, args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
		return
	}
	if rf.state != Leader || rf.currentTerm != term {
		return
	}

	if reply.Success {
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > rf.matchIndex[peer] {
			rf.matchIndex[peer] = newMatch
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.advanceCommitIndexLocked()
		return
	}

	// failure: ignore if nextIndex already moved since we built this request
	if rf.nextIndex[peer] != args.PrevLogIndex+1 {
		return
	}
	if reply.ConflictTerm == 0 {
		// follower's log is too short
		rf.nextIndex[peer] = reply.ConflictIndex
	} else {
		// does our own log contain the conflicting term?
		lastIdx := 0
		for i := rf.lastIncludedIndex + len(rf.log); i > rf.lastIncludedIndex; i-- {
			t := rf.termAt(i)
			if t == reply.ConflictTerm {
				lastIdx = i
				break
			}
			if t < reply.ConflictTerm {
				break
			}
		}
		if lastIdx > 0 {
			rf.nextIndex[peer] = lastIdx + 1
		} else {
			rf.nextIndex[peer] = reply.ConflictIndex
		}
	}
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}

}

func (rf *Raft) applier() {
	for true {
		rf.mu.Lock()
		var msgs []raftapi.ApplyMsg
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied-rf.lastIncludedIndex-1].Command,				
				CommandIndex: rf.lastApplied,
			})
		}
		rf.mu.Unlock()

		for _, msg := range msgs {
			rf.applyCh <- msg
		}

		time.Sleep(10*time.Millisecond)
	}
}
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = Follower
	rf.electionDeadline = time.Now()

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}
