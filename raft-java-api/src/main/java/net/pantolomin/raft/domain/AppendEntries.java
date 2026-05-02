package net.pantolomin.raft.domain;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

/**
 * Invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
 */
@Builder
@Getter
@ToString
public class AppendEntries implements Serializable {
    /**
     * Leader’s term
     */
    private final int term;

    /**
     * So follower can redirect clients
     */
    private final int leaderId;

    /**
     * Index of log entry immediately preceding new ones
     */
    private final int prevLogIndex;

    /**
     * Term of prevLogIndex entry
     */
    private final int prevLogTerm;

    /**
     * Log entries to store (empty for heartbeat; may send more than one for efficiency)
     */
    private final LogEntry[] entries;

    /**
     * Leader’s commitIndex
     */
    private final int leaderCommit;

    /**
     * @param term    CurrentTerm, for leader to update itself
     * @param success true if follower contained entry matching prevLogIndex and prevLogTerm
     */
    public record Response(int term, boolean success) implements Serializable {
        // nothing else needed
    }
}
