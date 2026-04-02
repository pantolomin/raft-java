package net.pantolomin.raft.domain;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

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

    @RequiredArgsConstructor
    @Getter
    public static class Response {
        /**
         * CurrentTerm, for candidate to update itself
         */
        private final int term;

        /**
         * true means candidate received vote
         */
        private final boolean voteGranted;
    }
}
