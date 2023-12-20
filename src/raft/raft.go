package raft

//
// th216s is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type status int8

const (
	follower status = iota
	candidate
	leader
)

type logEntry struct {
	Idx     int // log index w.r.t. log[]
	Term    int // term of the leader which commited the log
	Command interface{}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex // Lock to protect shared access to this peer's state
	condMu    sync.Mutex
	cond      *sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int // initialized to 0
	VotedFor    int
	Log         []*logEntry
	commitIndex int // initialized to 0
	lastApplied int // initialized to 0
	// consistency check optimization
	// termFirstIndxMap map[int]int // keep track of the first index for each term
	// termLastIndxMap  map[int]int // keep track of the last index for each term
	//(2D)
	LastIncludedIndex int // initialized to 0, storing the data for indexing purpose
	LastIncludedTerm  int // initialized to 0

	// leader only
	nextIndex  []int
	matchIndex []int // initialized to 0

	leaderID      int // initialized to -1
	status        status
	hbs           chan bool // heartbeat channel
	electionTimer *time.Timer
	applyCh       chan ApplyMsg
	leaderCommit  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.RLock()
	defer rf.mu.RUnlock()
	var term int = rf.CurrentTerm
	var isleader bool
	// Your code here (2A).
	if rf.status == leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) getStateBuffer() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.mu.RLock()
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	if rf.lastApplied-rf.LastIncludedIndex >= 0 && len(rf.Log) >= rf.lastApplied-rf.LastIncludedIndex {
		e.Encode(rf.Log[:rf.lastApplied-rf.LastIncludedIndex])
	} else {
		e.Encode(rf.Log)
	}
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	rf.mu.RUnlock()
	data := w.Bytes()
	return data
}
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.getStateBuffer())
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	rf.persister.SaveStateAndSnapshot(rf.getStateBuffer(), snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var Log []*logEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&Log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		log.Fatal("Decoding error")
	} else {
		rf.mu.Lock()
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = Log
		rf.LastIncludedIndex = lastIncludedIndex
		rf.LastIncludedTerm = lastIncludedTerm
		rf.mu.Unlock()
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.RLock()
	lastApplied := rf.lastApplied
	rf.mu.RUnlock()

	if lastApplied < lastIncludedIndex {
		rf.mu.Lock()
		rf.lastApplied = lastIncludedIndex
		rf.mu.Unlock()
		return true
	} else {
		return false
	}

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if !rf.killed() {
		// Your code here (2D).
		rf.mu.Lock()
		if index <= rf.LastIncludedIndex {
			Debug(dSnap, "S%d received stale snapshot from service", rf.me)
			rf.mu.Unlock()
			return
		}
		Debug(dSnap, "S%d is snapshoting at index %d", rf.me, index)
		term := rf.Log[index-rf.LastIncludedIndex-1].Term
		rf.Log = rf.Log[index-rf.LastIncludedIndex:]
		rf.LastIncludedIndex = index
		rf.LastIncludedTerm = term
		status := rf.status
		rf.mu.Unlock()

		rf.persistStateAndSnapshot(snapshot)
		//Debug(dSnap, "S%d is sending snapshot at index %d", rf.me, index)
		go func() {
			rf.applyCh <- ApplyMsg{SnapshotValid: true, SnapshotTerm: term, SnapshotIndex: index, Snapshot: snapshot}
		}()
		//Debug(dSnap, "S%d sent snapshot at index %d", rf.me, index)
		if status == leader {
			args := InstallSnapshotArgs{rf.CurrentTerm, rf.me, index, term, snapshot}
			reply := InstallSnapshotReply{}
			for i := 0; i < len(rf.peers); i++ {
				go func(i int) {
					rf.mu.RLock()
					nextIdx := rf.nextIndex[i]
					lastIncludedIndex := rf.LastIncludedIndex
					rf.mu.RUnlock()
					ok := false
					for i != rf.me && nextIdx <= index && lastIncludedIndex <= index && !ok {
						go func(i int) {
							Debug(dSnap, "S%d is sending snapshot to S%d", rf.me, i)
							ok = rf.peers[i].Call("Raft.InstallSnapshot", &args, &reply)
						}(i)
						time.Sleep(20 * time.Millisecond)

						if ok {
							Debug(dSnap, "S%d leader is checking the snapshot reply", rf.me)
							if reply.Term > rf.CurrentTerm {
								rf.mu.Lock()
								rf.status = follower
								rf.leaderID = -1
								rf.CurrentTerm = reply.Term
								Debug(dLeader, "S%d Leader, becomes follower when sending snapshot", rf.me)
								rf.mu.Unlock()
								rf.persist()
							} else {
								rf.mu.Lock()
								rf.nextIndex[i] = lastIncludedIndex + 1
								rf.mu.Unlock()
							}
						}
						time.Sleep(600 * time.Millisecond)
						rf.mu.RLock()
						nextIdx = rf.nextIndex[i]
						lastIncludedIndex = rf.LastIncludedIndex
						rf.mu.RUnlock()
					}
				}(i)
			}

		}
	}

}

type InstallSnapshotArgs struct {
	Term              int // leader's term
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int //current term
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.RLock()
	Debug(dSnap, "S%d received snapshot from leader S%d at index %d", rf.me, args.LeaderId, args.LastIncludedIndex)
	reply.Term = rf.CurrentTerm
	if args.Term < reply.Term {
		rf.mu.RUnlock()
		return
	}
	//truncateIndex := len(rf.Log)
	overLap := false
	if len(rf.Log)-1 >= 0 && args.LastIncludedIndex <= rf.Log[len(rf.Log)-1].Idx &&
		args.LastIncludedIndex >= rf.LastIncludedIndex &&
		rf.Log[args.LastIncludedIndex-rf.LastIncludedIndex-1].Term == args.LastIncludedTerm {
		// the snapshot overlaps with rf.Log
		overLap = true
	}
	rf.mu.RUnlock()

	rf.mu.Lock()
	if !overLap {
		rf.Log = make([]*logEntry, 0)
	} else {
		rf.Log = rf.Log[args.LastIncludedIndex-rf.LastIncludedIndex:]
	}
	rf.LastIncludedIndex = args.LastIncludedIndex
	rf.LastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = args.LastIncludedIndex
	rf.mu.Unlock()
	rf.persistStateAndSnapshot(args.Data)
	rf.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotTerm: args.LastIncludedTerm, SnapshotIndex: args.LastIncludedIndex}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int //candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int // current term
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.RLock()
	curTerm := rf.CurrentTerm
	logLength := len(rf.Log)
	rf.mu.RUnlock()
	reply.Term = curTerm
	reply.VoteGranted = false
	if args.Term >= curTerm {
		rf.mu.RLock()
		rf.CurrentTerm = args.Term
		rf.status = follower
		if args.Term > curTerm {
			rf.VotedFor = -1
		}
		votedFor := rf.VotedFor
		lastCommitedLogIndex := 0
		lastCommitedLogTerm := 0
		if logLength > 0 && rf.commitIndex-rf.LastIncludedIndex > 0 {
			lastCommitedLogIndex = rf.commitIndex
			lastCommitedLogTerm = rf.Log[rf.commitIndex-rf.LastIncludedIndex-1].Term
		}
		rf.mu.RUnlock()
		//if (!exists || votedFor == args.CandidateId) &&
		if (votedFor == -1 || votedFor == args.CandidateId) && checkCandidateLogUpToDate(args.LastLogIndex, args.LastLogTerm, lastCommitedLogIndex, lastCommitedLogTerm) {
			reply.VoteGranted = true
			rf.mu.Lock()
			rf.VotedFor = args.CandidateId
			rf.mu.Unlock()
			electionTimeout := time.Duration(rand.Intn(500)+1000) * time.Millisecond // 1000ms ~ 1500ms
			if !rf.electionTimer.Stop() {
				<-rf.electionTimer.C
			}
			rf.electionTimer.Reset(electionTimeout)
		}
		rf.persist()

	}

}

func checkCandidateLogUpToDate(candidateLogIdx int, candidateLogTerm int, lastCommitedLogIndex int, lastCommitedLogTerm int) bool {
	// (2B) log-based rejection
	if candidateLogTerm < lastCommitedLogTerm {
		return false
	} else if candidateLogTerm > lastCommitedLogTerm {
		return true
	} else {
		if candidateLogIdx >= lastCommitedLogIndex {
			return true
		} else {
			return false
		}
	}
}

type AppendEntriesArg struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*logEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int // currentTerm
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	logCheck := false

	rf.mu.RLock()
	curTerm := rf.CurrentTerm
	lastIncludedIndex := rf.LastIncludedIndex
	lastIncludedTerm := rf.LastIncludedTerm
	logCopy := make([]*logEntry, len(rf.Log))
	copy(logCopy, rf.Log)
	commitIndex := rf.commitIndex
	rf.mu.RUnlock()

	if args.PrevLogIndex-lastIncludedIndex == 0 && args.PrevLogTerm == lastIncludedTerm {
		// leader doesn't have previous log entry
		logCheck = true
	} else if args.PrevLogIndex-lastIncludedIndex > 0 && len(logCopy) >= args.PrevLogIndex-lastIncludedIndex {
		temp := logCopy[args.PrevLogIndex-lastIncludedIndex-1]
		if temp.Term == args.PrevLogTerm {
			logCheck = true
		}
	}

	reply.Term = curTerm
	if args.Term < curTerm {
		Debug(dLeader, "S%d has received stale heartbeat from S%d", rf.me, args.LeaderId)
		reply.Success = false
	} else if !logCheck {
		reply.Success = false
		if args.PrevLogIndex < lastIncludedIndex {
			reply.ConflictIndex = lastIncludedIndex + 1
		} else if len(logCopy) < args.PrevLogIndex-lastIncludedIndex {
			// follower has several missing entries
			if len(logCopy) == 0 {
				reply.ConflictIndex = lastIncludedIndex + 1
			} else {
				reply.ConflictIndex = logCopy[len(logCopy)-1].Idx + 1
			}

			reply.ConflictTerm = 0
		} else {
			reply.ConflictTerm = logCopy[args.PrevLogIndex-lastIncludedIndex-1].Term
			reply.ConflictIndex = args.PrevLogIndex
			i := args.PrevLogIndex - lastIncludedIndex
			for ; i >= 1; i-- {
				if logCopy[i-1].Term != reply.ConflictTerm {
					reply.ConflictIndex = logCopy[i-1].Idx + 1
					break
				}
			}
			if logCopy[0].Term == reply.ConflictTerm && lastIncludedTerm != 0 && lastIncludedTerm != reply.ConflictTerm {
				// check if the leader should send all the logs
				reply.ConflictIndex = logCopy[0].Idx
			}

		}

		rf.hbs <- true
		rf.mu.Lock()
		rf.CurrentTerm = args.Term
		rf.leaderID = args.LeaderId
		rf.status = follower
		Debug(dLog, "S%d has inconsistent entry %d from leader S%d", rf.me, reply.ConflictIndex, args.LeaderId)
		rf.mu.Unlock()
		rf.persist()
	} else {
		reply.Success = true
		rf.hbs <- true
		rf.mu.Lock()
		rf.leaderID = args.LeaderId
		rf.status = follower
		if args.Term > curTerm {
			rf.VotedFor = -1
		}
		rf.CurrentTerm = args.Term
		conflictingIdx := 0
		Debug(dLog, "S%d has received heartbeat from S%d with %d entries", rf.me, args.LeaderId, len(args.Entries))
		if len(args.Entries) > 0 {
			for i, logEntry := range args.Entries {
				logId := logEntry.Idx - lastIncludedIndex
				if len(logCopy) >= logId && logCopy[logId-1].Term != logEntry.Term {
					// if any existing entry conflicts with a new one,
					// mark the entry idx and delete all the entries that follow it
					conflictingIdx = i
					rf.Log = logCopy[:(logId - 1)]
					break
				}
			}
			for i := conflictingIdx; i < len(args.Entries); i++ {
				if len(rf.Log) < args.Entries[i].Idx-rf.LastIncludedIndex {
					rf.Log = append(rf.Log, args.Entries[i])
				}
			}
			Debug(dLog, "S%d has log of length %v", rf.me, len(rf.Log))
		}
		if args.LeaderCommit > commitIndex {
			if len(args.Entries) > 0 {
				rf.commitIndex = min(args.LeaderCommit, args.Entries[len(args.Entries)-1].Idx)
			} else {
				rf.commitIndex = args.LeaderCommit
			}

			// Debug(dCommit, "S%d has commitIndex=%v, lastAppliedIndex=%v",
			// 	rf.me, rf.commitIndex, rf.lastApplied)
		}
		rf.leaderCommit = args.LeaderCommit
		rf.mu.Unlock()
		rf.cond.Signal()
		rf.persist()

	}
}

func (rf *Raft) sendAppendEntries() {
	rf.mu.RLock()
	status := rf.status
	rf.mu.RUnlock()
	for !rf.killed() && status == leader {
		var wg sync.WaitGroup
		Debug(dLog, "S%d has nextIndex array:%v", rf.me, rf.nextIndex)
		rf.mu.RLock()
		curTerm := rf.CurrentTerm
		logCopy := make([]*logEntry, len(rf.Log))
		nextIndexCopy := make([]int, len(rf.nextIndex))
		copy(logCopy, rf.Log)
		copy(nextIndexCopy, rf.nextIndex)
		rf.mu.RUnlock()
		count := 1
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				wg.Add(1)
				rf.mu.RLock()
				// initialize AppendEntriesArg assuming prevLog doesn't exist
				args := AppendEntriesArg{curTerm, rf.me, rf.LastIncludedIndex, rf.LastIncludedTerm, nil, rf.commitIndex}
				if nextIndexCopy[i]-rf.LastIncludedIndex > 1 {
					args.PrevLogIndex = nextIndexCopy[i] - 1
					args.PrevLogTerm = logCopy[args.PrevLogIndex-rf.LastIncludedIndex-1].Term
					// args.Entries could be empty
					args.Entries = logCopy[nextIndexCopy[i]-rf.LastIncludedIndex-1:]
				} else if nextIndexCopy[i] > rf.LastIncludedIndex {
					// no previous log or previous log index is equal to lastIncludedIndex
					args.Entries = logCopy
				} // else no entries to send and no previous log
				rf.mu.RUnlock()
				reply := AppendEntriesReply{}
				ok := false
				go func(i int) {
					ok = rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
				}(i)
				go func(i int) {
					time.Sleep(100 * time.Millisecond)
					if ok {
						rf.mu.Lock()
						if !reply.Success && reply.Term > rf.CurrentTerm {
							rf.status = follower
							rf.leaderID = -1
							rf.CurrentTerm = reply.Term
							Debug(dLeader, "S%d Leader, becomes follower", rf.me)
						} else if !reply.Success {
							// Follower's log is inconsistent with the leader's
							if nextIndexCopy[i] == rf.LastIncludedIndex+1 && reply.ConflictIndex == nextIndexCopy[i] {
								// This is when leader sent all the current log entries, but all of them conflict
								// Then send a snap shot to the follower
								// TO DO: not sure how to send a snapshot
								Debug(dSnap, "S%d needs a snapshot from leader S%d", i+1, rf.me)
								return
							}
							rf.nextIndex[i] = reply.ConflictIndex
							//if reply.ConflictTerm != 0 {
							// Search log for the last entry j with term conflictTerm
							// for j := len(rf.Log); j >= 1; j-- {
							// 	if rf.Log[j-1].Term == reply.ConflictTerm {
							// 		rf.nextIndex[i] = j+rf.LastIncludedIndex + 1
							// 	}
							// }

							// }

						} else {
							// Success
							if len(logCopy) >= 1 && nextIndexCopy[i] <= logCopy[len(logCopy)-1].Idx {
								rf.matchIndex[i] = logCopy[len(logCopy)-1].Idx
								rf.nextIndex[i] = logCopy[len(logCopy)-1].Idx + 1
								count++
							}
						}
						rf.mu.Unlock()

					}
					wg.Done()
				}(i)
			}
		}

		wg.Wait()

		rf.mu.Lock()
		if count > len(rf.peers)/2 {
			for N := len(logCopy); N > rf.commitIndex-rf.LastIncludedIndex; N-- {
				if logCopy[N-1].Term == rf.CurrentTerm {
					// Figure 8, only commits entries from current term
					rf.commitIndex = logCopy[N-1].Idx
					break
				}
			}
			rf.leaderCommit = rf.commitIndex
			Debug(dCommit, "S%d leader has commited up to index %d, lastIncludedIndex=%v, lastAppliedIndex=%v", rf.me, rf.commitIndex, rf.LastIncludedIndex, rf.lastApplied)
		}
		status = rf.status
		rf.mu.Unlock()
		rf.cond.Signal()
		rf.persist()

	}
}

func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.cond.L.Lock()
		for rf.commitIndex <= rf.lastApplied || len(rf.Log) == 0 || rf.Log[len(rf.Log)-1].Idx < rf.commitIndex {
			rf.cond.Wait()
		}

		rf.cond.L.Unlock()

		rf.mu.RLock()
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		rf.mu.RUnlock()
		for i := lastApplied + 1; i <= commitIndex; i++ {
			// apply log[rf.lastApplied+1,... rf.leaderCommit]
			rf.mu.Lock()
			command := rf.Log[i-rf.LastIncludedIndex-1].Command
			rf.lastApplied++
			rf.mu.Unlock()
			rf.applyCh <- ApplyMsg{SnapshotValid: false, CommandValid: true, Command: command, CommandIndex: i}

		}

		Debug(dSnap, "S%d has applied log up to index of %d, where commitIndex=%d", rf.me, rf.lastApplied, commitIndex)
	}
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
	ok := false
	if !rf.killed() {
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := 0

	// Your code here (2B).
	term, isLeader := rf.GetState()
	if isLeader {
		rf.mu.Lock()
		logentry := logEntry{len(rf.Log) + 1 + rf.LastIncludedIndex, term, command} // logEntry is 1-indexed
		rf.Log = append(rf.Log, &logentry)
		index = logentry.Idx
		Debug(dCommit, "S%d leader received the command %d at index %d from client", rf.me, command, index)
		rf.mu.Unlock()
		rf.persist()
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.RLock()
		status := rf.status
		rf.mu.RUnlock()
		if status == follower {
			// Your code here to check if a leader election should
			// be started and to randomize sleeping time using
			// time.Sleep().
			electionTimeout := time.Duration(rand.Intn(500)+1000) * time.Millisecond // 250ms ~ 400ms
			if !rf.electionTimer.Stop() {
				<-rf.electionTimer.C
			}
			rf.electionTimer.Reset(electionTimeout)
			select {
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				rf.status = candidate
				Debug(dTerm, "S%d follower is starting an election for term %d", rf.me, rf.CurrentTerm+1)
				rf.mu.Unlock()
				if !rf.killed() {
					rf.startElection()
				}

			case <-rf.hbs:

			}
		}
		time.Sleep(10 * time.Millisecond)

	}
}

func (rf *Raft) startElection() {
	// Start another timer for receiving a majority of votes
	electionTimeout := time.Duration(rand.Intn(500)+1000) * time.Millisecond // 1000ms ~ 1500ms
	rf.electionTimer.Reset(electionTimeout)
	// call request vote
	rf.mu.Lock()
	rf.CurrentTerm++
	currentTerm := rf.CurrentTerm
	rf.VotedFor = rf.me
	rf.mu.Unlock()
	rf.mu.RLock()
	lastLogIdx := len(rf.Log) + rf.LastIncludedIndex
	// initialize args assuming rf.Log is empty
	args := RequestVoteArgs{currentTerm, rf.me, rf.LastIncludedIndex, rf.LastIncludedTerm}
	if lastLogIdx > rf.LastIncludedIndex {
		args.LastLogIndex = lastLogIdx
		args.LastLogTerm = rf.Log[lastLogIdx-rf.LastIncludedIndex-1].Term
	}

	rf.mu.RUnlock()
	var vote uint32 = 1 // vote for self
	// send request vote RPCs to all other servers\
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			reply := RequestVoteReply{}
			go func(i int) {
				ok := rf.sendRequestVote(i, &args, &reply)
				if ok {
					if reply.VoteGranted {
						atomic.AddUint32(&vote, 1)
					}
				}
				//Debug(dVote, "S%d received vote request reply from %d", rf.me, i)

			}(i)
		}
	}
	elected := make(chan struct{})
	defer close(elected)
	done := false

	//Debug(dVote, "S%d received %v votes in term %v. There's %v alive\n", rf.me, vote, currentTerm, atomic.LoadUint32(&rf.nPeers))
	go func() {
		for !rf.killed() {
			if atomic.LoadUint32(&vote) > uint32(len(rf.peers)/2) {
				// there should not be more than one machine to elect a leader
				rf.mu.RLock()
				mostRecentTerm := rf.CurrentTerm
				rf.mu.RUnlock()
				if currentTerm >= mostRecentTerm && !done {
					elected <- struct{}{}
				}
				break
			}
			time.Sleep(10 * time.Microsecond)
		}
	}()

	select {
	case <-elected:
		rf.mu.Lock()
		rf.leaderID = rf.me
		rf.status = leader
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.commitIndex + 1
		}
		Debug(dVote, "S%d is elected for term %v\n", rf.me, rf.CurrentTerm)
		rf.mu.Unlock()
		go rf.sendAppendEntries()

	case <-rf.hbs:
		done = true
		rf.mu.Lock()
		rf.status = follower
		//Debug(dVote, "S%d candidate received a heart beat from leader %v for term %v\n", rf.me, rf.leaderID, rf.currentTerm)
		rf.mu.Unlock()
	case <-rf.electionTimer.C:
		// election timed out, start a new one
		done = true
		if !rf.killed() {
			rf.startElection()
			Debug(dVote, "S%d election is timed out, starting a new one at term %d", rf.me, currentTerm)
		}

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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.hbs = make(chan bool, 1)
	rf.electionTimer = time.NewTimer(0) //initialize a timer
	rf.applyCh = applyCh
	rf.cond = sync.NewCond(&rf.condMu)

	// Your initialization code here (2A, 2B, 2C).

	rf.Log = make([]*logEntry, 0)
	rf.CurrentTerm = 0 // initialized to 0
	rf.VotedFor = -1   // intialized to an empty map
	rf.commitIndex = 0 // initialized to -1
	rf.lastApplied = 0 // initialized to -1
	rf.leaderID = -1
	rf.status = follower
	rf.leaderCommit = 0

	rf.LastIncludedIndex = 0
	rf.LastIncludedTerm = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = len(rf.Log) + rf.LastIncludedIndex
	// TO DO: how to reload rf.lastApplied, rf.leaderCommit
	rf.lastApplied = rf.commitIndex
	rf.leaderCommit = rf.commitIndex

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLog()

	return rf
}
