package net.pantolomin.raft.domain;

import lombok.Builder;
import lombok.Getter;

/**
 * Invoked by candidates to gather votes (§5.2).
 */
@Getter
@Builder
public class RequestVote {
    /**
     * Candidate’s term
     */
    private final int term;

    /**
     * Candidate requesting vote
     */
    private final int candidateId;

    /**
     * Index of candidate’s last log entry (§5.4)
     */
    private final int lastLogIndex;

    /**
     * Term of candidate’s last log entry (§5.4)
     */
    private final int lastLogTerm;

    /**
     * @param term        CurrentTerm, for candidate to update itself
     * @param voteGranted true means candidate received vote
     */
    public record Response(int term, boolean voteGranted) {
        // nothing else needed
    }
}
