package net.pantolomin.raft.domain;

import lombok.Getter;
import lombok.Setter;
import net.pantolomin.raft.Log;

@Getter
public class State {
    // ************************************************************************
    // Persistent state on all servers:
    // (Updated on stable storage before responding to RPCs)
    // ************************************************************************

    /**
     * Log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
     */
    private final Log log = new Log();

    /**
     * Latest term server has seen (initialized to 0 on first boot, increases monotonically)
     */
    @Setter
    private int currentTerm;

    /**
     * CandidateId that received vote in current term (or null if none)
     */
    private int votedFor;

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
