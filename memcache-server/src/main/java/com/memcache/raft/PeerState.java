package com.memcache.raft;

public class PeerState {
    /*
    @param matchIndex : index which is confirmed replicated in peers log, not necessary to be applied in state machine.
    @param nextIndex : index till which the peer has seen the logs
    these are not final because they will keep on changing as we go through the log
     */
    private int matchIndex;
    private int nextIndex;

    // at start the match index will start from 0 and next index will be the leaders last log index + 1 (optimistic approach)
    public PeerState(int nextIndex){
        this.matchIndex = 0;
        this.nextIndex = nextIndex;
    }

    public int getMatchIndex() {
        return matchIndex;
    }

    public int getNextIndex() {
        return nextIndex;
    }

    public void setMatchIndex(int matchIndex) {
        this.matchIndex = matchIndex;
    }

    public void setNextIndex(int nextIndex) {
        this.nextIndex = nextIndex;
    }
}

/*

RequestVoteRequest 
 its sent by the candidate to the follower/candidate to ask for votes
 if a node is asking for votes then it should tell what is node id, last log index and last log term so that other followers can check whether
 the cadidate which they are supposed to vote for has updated log

 RequestVoteResponse
 its sent by follower to candidate to tell what is the response of its vote request  if its granted then it will tell using boolean field

 AppendEntriesRequest
 its sent by leader to follower to append these entries to their log
 a append entry must have leaderId (id of the leader) , prevLogIndex(according to leader, a follower has previous log entries till this index), 
 prevLogTerm(according to leader, a follower has previous log entries till this term), commitIndex( so that follower knows till what index, our current cluster has entries commited)
 entries(list of entries to be appended to the log) , 


AppendEntriesResponse
its sent by follower to leader to tell whether it has appended the entries or not
if it has appended the entries then it will sent ACK else NACK we can sent this as true or false as response to denote whether it has appended the entries or not


persistent fields which should survive after node restart are
1. term (int) because if we start our term from 0 then even though we might have info till actual term - 2 still we will have to start from term 0 again
2. votedFor (int) :- so that we dont wont in same term twice, to avoid vote manipulation
3. log (list of entries) :- logs are appended to this list this act as our datastore

volatile fields which are allowed to get lost after node restart are 
// 1. prevLogTerm(int) :- this is allowed to get lost after node restart because we can check our log and find out what the term of most recent log entry
// 2. prevLogIndex(int) :- this is allowed to get lost after node restart because we can check our log and find out what the index of most recent log entry
prevLogterm and prevLogIndex can be calculated by looking at peerstate 

4. commitIndex(int) :- so that we know till what index , we were in sync with our cluster
5. leaderId(int)/candidateId(int) :- so that we know who is this candidate , just like name of person
3. appliedIndex(int) :- this is allowed to get lost because once we restore our log we can set applied index to size of log.
5. role : role of the node

leader only fields
// 1. nextIndex(int) :- this is the index till which the leader has seen the logs
// 2. matchIndex(int) :- this is the index which is confirmed replicated in peers log, not necessary to be applied in state machine.
we use peers <string, peerstate> 
timers
1. electionTimeout(int) :- after this time, a candidate will start the election process
2. heartbeatTimeout(int) :- after this time, a leader will send heartbeat to follower

external dependencies
1. cluster configuration :- like how many nodes are in cluster and id of each of them so that it can communicate with other nodes

 */
