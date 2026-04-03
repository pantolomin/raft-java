package net.pantolomin.raft.domain;

import lombok.Getter;
import net.pantolomin.raft.Log;

@Getter
public class State {
    // ************************************************************************
    // Persistent state on all servers:
    // (Updated on stable storage before responding to RPCs)
    // ************************************************************************

    /**
     * Latest term server has seen (initialized to 0 on first boot, increases monotonically)
     */
    private int currentTerm;

    /**
     * CandidateId that received vote in current term (or null if none)
     */
    private int votedFor;

    /**
     * Log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
     */
    private Log log;

    // ************************************************************************
    // Volatile state on all servers:
    // ************************************************************************

    /**
     * Index of highest log entry known to be committed (initialized to 0, increases monotonically)
     */
    private int commitIndex;

    /**
     * Index of highest log entry applied to state machine (initialized to 0, increases monotonically)
     */
    private int lastApplied;

    // ************************************************************************
    // Volatile state on leaders:
    // (Reinitialized after election)
    // ************************************************************************

    /**
     * For each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
     */
    private int[] nextIndex;

    /**
     * For each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
     */
    private int[] matchIndex;

    // ************************************************************************
    // State mutations
    // ************************************************************************

    public void incrementTerm() {
        this.currentTerm++;
    }

    public void becomeCandidate() {
        this.votedFor = 1;
    }

    public int addVote() {
        return ++this.votedFor;
    }

    public void add(LogEntry entry) {
        this.log.add(entry);
    }
}
