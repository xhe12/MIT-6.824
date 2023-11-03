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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	cond      *sync.Cond
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int // initialized to 0
	votedFor    map[int]int
	//log []*logEntry
	commitIndex int // initialized to 0
	lastApplied int // initialized to 0

	// leader only
	//nextIndex  []int
	//matchIndex []int // initialized to 0

	leaderID      int // initialized to -1
	status        status
	hbs           chan bool // heartbeat channel
	nPeers        uint32    // number of peers that are alive (responds to requestVote RPC)
	electionTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int = rf.currentTerm
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
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate's term
	CandidateId int //candidate requesting vote
	// TO DO: lastLogIndex
	// TO DO: lastLogTerm
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
	rf.mu.Lock()
	curTerm := rf.currentTerm
	rf.mu.Unlock()
	reply.Term = curTerm
	reply.VoteGranted = false
	if args.Term > curTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.status = follower
		votedFor, exists := rf.votedFor[args.Term]
		rf.mu.Unlock()
		if !exists || votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.mu.Lock()
			rf.votedFor[args.Term] = args.CandidateId
			rf.mu.Unlock()
			electionTimeout := time.Duration(rand.Intn(350)+250) * time.Millisecond // 250ms ~ 400ms
			if !rf.electionTimer.Stop() {
				<-rf.electionTimer.C
			}
			rf.electionTimer.Reset(electionTimeout)
		}

	}
	// (2B) TO DO: log-based rejection
}

type AppendEntriesArg struct {
	Term     int // leader's term
	LeaderId int
	//PrevLogIndex int
	//PrevLogTerm int
	//Entries []Entry
	//LeaderCOmmit int
}

type AppendEntriesReply struct {
	Term    int // currentTerm
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else {
		reply.Success = true
		rf.hbs <- true
		Debug(dTerm, "S%d has received heartbeat from S%d in term %v\n", rf.me, args.LeaderId, rf.currentTerm)
		rf.leaderID = args.LeaderId
		rf.status = follower
		rf.currentTerm = args.Term
		//rf.cond.Broadcast()
	}
}
func (rf *Raft) sendAppendEntries() {
	for !rf.killed() {
		rf.mu.Lock()
		status := rf.status
		rf.mu.Unlock()
		for status == leader {
			args := AppendEntriesArg{rf.currentTerm, rf.me}
			//replies := make(chan AppendEntriesReply)
			var wg sync.WaitGroup
			Debug(dTimer, "S%d Leader, checking heartbeats", rf.me)
			rf.mu.Lock()
			curTerm := rf.currentTerm
			rf.mu.Unlock()
			replied := make(chan bool)
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					wg.Add(1)
					reply := AppendEntriesReply{}
					go func(i int) {
						replied <- rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
					}(i)
					select {
					case <-time.After(time.Millisecond * 2):
						//
					case ok := <-replied:
						if ok {
							if reply.Term > curTerm {
								rf.mu.Lock()
								rf.status = follower
								rf.leaderID = -1
								rf.currentTerm = reply.Term
								rf.mu.Unlock()
								Debug(dLeader, "S%d Leader, becomes follower", rf.me)
							}
						}
					}
					wg.Done()

				}
			}
			wg.Wait() // wait for all the calls to complete

			rf.mu.Lock()
			status = rf.status
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)

		}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
		rf.mu.Lock()
		status := rf.status
		rf.mu.Unlock()
		if status == follower {
			// Your code here to check if a leader election should
			// be started and to randomize sleeping time using
			// time.Sleep().
			electionTimeout := time.Duration(rand.Intn(200)+250) * time.Millisecond // 250ms ~ 400ms
			if !rf.electionTimer.Stop() {
				<-rf.electionTimer.C
			}
			rf.electionTimer.Reset(electionTimeout)
			select {
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				rf.status = candidate
				Debug(dTerm, "S%d follower is starting an election for term %d", rf.me, rf.currentTerm+1)
				rf.mu.Unlock()
				rf.startElection()
			case <-rf.hbs:

			}
		}

	}
}

func (rf *Raft) startElection() {
	// Start another timer for receiving a majority of votes
	electionTimeout := time.Duration(rand.Intn(150)+250) * time.Millisecond // 250ms ~ 400ms
	rf.electionTimer.Reset(electionTimeout)
	// call request vote
	rf.mu.Lock()
	rf.currentTerm++
	currentTerm := rf.currentTerm
	rf.votedFor[currentTerm] = rf.me
	rf.mu.Unlock()
	var vote uint32 = 1 // vote for self
	// send request vote RPCs to all other servers
	args := RequestVoteArgs{currentTerm, rf.me}

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

				Debug(dVote, "S%d received vote request reply from %d", rf.me, i)

			}(i)
		}
	}
	elected := make(chan struct{})
	defer close(elected)
	done := false

	//wg.Wait()

	//Debug(dVote, "S%d received %v votes in term %v. There's %v alive\n", rf.me, vote, currentTerm, atomic.LoadUint32(&rf.nPeers))
	go func() {
		for {
			if atomic.LoadUint32(&vote) > uint32(len(rf.peers)/2) {
				// there should not be more than one machine to elect a leader
				rf.mu.Lock()
				if currentTerm >= rf.currentTerm && !done {
					elected <- struct{}{}
				}
				rf.mu.Unlock()
				break
			}
		}
	}()

	select {
	case <-elected:
		rf.mu.Lock()
		rf.leaderID = rf.me
		rf.status = leader
		Debug(dVote, "S%d is elected for term %v\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		go rf.sendAppendEntries()

	case <-rf.hbs:
		done = true
		rf.mu.Lock()
		rf.status = follower
		Debug(dVote, "S%d received a heart beat from leader %v for term %v\n", rf.me, rf.leaderID, rf.currentTerm)
		rf.mu.Unlock()
	case <-rf.electionTimer.C:
		// election timed out, start a new one
		done = true
		Debug(dVote, "S%d election is timed out, starting a new one...", rf.me)
		rf.startElection()
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
	rf.cond = sync.NewCond(&rf.mu)
	rf.hbs = make(chan bool, 20)
	rf.electionTimer = time.NewTimer(0) //initialize a timer

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0              // initialized to 0
	rf.votedFor = make(map[int]int) // intialized to an empty map
	rf.commitIndex = 0              // initialized to 0
	rf.lastApplied = 0              // initialized to 0
	rf.leaderID = -1
	rf.status = follower
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
