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
type NodeState int

const (
	Follower NodeState = iota
	Leader
	Candidate
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state NodeState

	currentTerm int
	votedFor int
	log []LogEntry

	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int

	nextHeartbeat time.Time
	killTicker bool //set to true if state = Leader
	// requestVoteReplySlice []RequestVoteReply{}
}

type LogEntry struct{
	term int
	command interface{}
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
	if rf.state == Leader {
		isleader = true
		rf.killTicker = true
	}
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
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)

}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	var votedFor int
	var log []LogEntry

	if d.Decode(&term) != nil || 
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		return
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = log
	}

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
	// Your code here (3D).

}

type AppendEntriesArgs struct{
    Term int
    LeaderId int
    PrevLogIndex int
    PrevLogTerm int
    Entries []LogEntry
    LeaderCommit int
}

type AppendEntriesReply struct{
    Term int
    Success bool
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	
	lastLogIndex := 0
    lastLogTerm := 0

    if len(rf.log) > 0 {
        lastLogIndex = len(rf.log) - 1
        lastLogTerm = rf.log[lastLogIndex].term
    }

	logOk := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rf.votedFor == -1 || 
		rf.votedFor == args.CandidateId) &&
		logOk {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.Term = args.Term
			reply.VoteGranted = true
			
			rf.persist()
	}

}


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
        rf.state = Follower
        rf.votedFor = -1
	
		rf.persist()
    }

    if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
        reply.Term = rf.currentTerm
		reply.Success = false
		return
    }

    // Rule 3 from paper
    for i, entry := range args.Entries {
        logIndex := args.PrevLogIndex + 1 + i
        if logIndex < len(rf.log) && rf.log[logIndex].Term != entry.Term {
            rf.log = rf.log[:logIndex] // delete from conflict point onwards
			break
        }
    }

    // Rule 4 form paper
    for i, entry := range args.Entries {
        logIndex := args.PrevLogIndex + 1 + i
        if logIndex >= len(rf.log) {
            rf.log = append(rf.log, entry)
		}
    }

    // Rule 5 — update commitIndex
    if args.LeaderCommit > rf.commitIndex {
        rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
    }
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.persist()

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
	isLeader := false
	// Your code here (3B).	
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return index, term, isLeader 
	}

	index = len(rf.log) - 1
	term = rf.currentTerm
	isLeader = true

	rf.log = append(rf.log, LogEntry{term = term, command = command})
	rf.persist()
	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for !rf.killTicker {
		rf.mu.Lock()
		state := rf.state
		nextHeartbeat := rf.nextHeartbeat
		log := rf.log
		rf.mu.Unlock()
		// Your code here (3A)
		// Check if a leader election should be started.
		switch state {
		case Follower, Candidate:
			if time.Now().After(nextHeartbeat) {
				rf.startElection()
			}
		case Leader:
			//if received log
			//update nextHerrtbeat

			if time.Now().After(nextHeartbeat) {
				rf.sendHeartbeats()
				
				rf.mu.Lock()
				rf.nextHeartbeat = time.Now().Add(100 * time.Millisecond)
				rf.mu.Unlock()
			}

		}
		
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) startELection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm ++
	rf.votedFor = rf.me
	ms := 300 + (rand.Int63() % 200) 
	rf.nextHeartbeat = time.Now().Add(time.Duration(ms) * time.Millisecond)

	logNum := len(rf.log) - 1

	arg := RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: logNum,
		LastLogTerm: rf.log[logNum].Term , 
	}
	rf.mu.Unlock()
	peerNum := 5 // number of nodes 5 
	voteCount := 1
	// voteCh := make(chan bool, peerNum)
	// voteCh <- true
	for num := 0; num < peerNum; num++ { 
		if num != rf.me {
			go func(peer int) {
				reply := RequestVoteReply{}
				rf.sendRequestVote(peer, &arg, &reply)

				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.state = Follower
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					rf.persist()

					rf.mu.Unlock()
					return
				}
				
				if reply.VoteGranted {
					voteCount++
					if voteCount > len(rf.peers)/2 && rf.state == Candidate {
						rf.state = Leader
						// i need to make sure to not call the function while the struct is locked
						rf.sendHeartbeats() //function to be added
					}
				}
				rf.mu.Unlock()
			}(num)
		}
	}
}

func (rf *Raft) sendHeartbeats(){
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term : rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: len(rf.log) - 1,
		PrevLogTerm: rf.log[len(rf.log) - 1].Term,
		Entries: []Entries{},
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	peerNum := 5

	for num := 0; num < peerNum; num++ {
		if num != rf.me{
			go func(peer int){
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(peer, &args, &reply)
				if !ok {
					return
				}

				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.state = Follower
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					rf.persist()

					rf.mu.Unlock()
					return
				}

				//add the logic for if reply log intex is less then len(rf.log) - 1
				if rf.nextIndex[peer] < len(rf.log) - 1{
					//i need to make sure unblock blocked field if needed
					// i need to prepare parameters
					rf.sendAppendEntriesToPeer(peer)
				}
			}(num)

		}
	}
}

func (rf *Raft) sendAppendEntriesToPeer(peer int){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if peer == rf.me {
		return
	}

	prevIndex := rf.nextIndex[peer] - 1
	args := AppendEntriesArgs{
		Term: rf.currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm: rf.log[prevIndex].Term,
		Entries: rf.log[rf.nextIndex[peer]:],
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}

	rf.mu.Unlock()
	for {
		ok := rf.sendAppendEntries(peer, &args, &reply)
		if !ok {
			rf.mu.Lock()
			return
		}
		rf.mu.Lock()

		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.state = Follower
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.persist()
			return
		}
		if reply.Success {
			rf.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[peer] = rf.nextIndex[peer] - 1
			return
		}
		// failed: decrement, rebuild args, retry
		rf.nextIndex[peer]--
		if rf.nextIndex[peer] == 0 {
			return
		}
		prevIndex = rf.nextIndex[peer] - 1
		args.PrevLogIndex = prevIndex
		args.PrevLogTerm = rf.log[prevIndex].Term
		args.Entries = append([]LogEntry{}, rf.log[rf.nextIndex[peer]:]...)
		reply = AppendEntriesReply{}
		rf.mu.Unlock()
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
