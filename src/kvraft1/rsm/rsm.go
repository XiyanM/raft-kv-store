package rsm

import (
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type Op struct {
	Id  int64
	Req any
}

type result struct {
	err rpc.Err
	rep any
}

type waiter struct {
	id int64
	ch chan result
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int
	sm           StateMachine
	waiters      map[int]*waiter
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		waiters:      make(map[int]*waiter),
	}

	// after a restart, load the saved snapshot before any command is applied
	if snapshot := persister.ReadSnapshot(); len(snapshot) > 0 {
		sm.Restore(snapshot)
	}

	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	op := Op{Id: rand.Int63(), Req: req}

	// hold the lock across Start and the registration, so the reader
	// can't deliver this index's result before the waiter exists
	rsm.mu.Lock()
	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	// an older Submit still waiting on this index lost its op
	if old, ok := rsm.waiters[index]; ok {
		old.ch <- result{err: rpc.ErrWrongLeader}
	}
	w := &waiter{id: op.Id, ch: make(chan result, 1)}
	rsm.waiters[index] = w
	rsm.mu.Unlock()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case res := <-w.ch:
			return res.err, res.rep
		case <-ticker.C:
			curTerm, stillLeader := rsm.rf.GetState()
			if curTerm == term && stillLeader {
				continue
			}
			// leadership changed, give up on this index
			rsm.mu.Lock()
			if rsm.waiters[index] == w {
				delete(rsm.waiters, index)
			}
			rsm.mu.Unlock()
			select {
			case res := <-w.ch: // the reader got there first
				return res.err, res.rep
			default:
			}
			return rpc.ErrWrongLeader, nil
		}
	}
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if !ok {
				continue
			}
			rep := rsm.sm.DoOp(op.Req) // every server applies every committed op

			rsm.mu.Lock()
			if w, ok := rsm.waiters[msg.CommandIndex]; ok {
				delete(rsm.waiters, msg.CommandIndex)
				if w.id == op.Id {
					w.ch <- result{err: rpc.OK, rep: rep}
				} else {
					// a different op committed at this index: ours was lost
					w.ch <- result{err: rpc.ErrWrongLeader}
				}
			}
			rsm.mu.Unlock()

			// hand a snapshot to Raft when its saved state gets big
			if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
				rsm.rf.Snapshot(msg.CommandIndex, rsm.sm.Snapshot())
			}
		} else if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)

			// ops at or below the snapshot were applied without us seeing
			// their results, so any Submit still waiting on them must retry
			rsm.mu.Lock()
			for index, w := range rsm.waiters {
				if index <= msg.SnapshotIndex {
					delete(rsm.waiters, index)
					w.ch <- result{err: rpc.ErrWrongLeader}
				}
			}
			rsm.mu.Unlock()
		}
	}
}
