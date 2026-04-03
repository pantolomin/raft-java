package net.pantolomin.raft.replication;

import net.pantolomin.raft.Agent;
import net.pantolomin.raft.Log;
import net.pantolomin.raft.api.ClusterMember;
import net.pantolomin.raft.api.ConnectionManager;
import net.pantolomin.raft.domain.AppendEntries;
import net.pantolomin.raft.domain.LogEntry;
import net.pantolomin.raft.domain.State;

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import static java.util.Comparator.comparing;

final class MemberReplicationContext {
    private final int leaderId;
    private final ClusterMember targetMember;
    private final Agent agent;
    private final ConnectionManager connectionManager;
    private final State state;
    private final Queue<MatchListener> matchListeners;

    private int nextIndex;
    private int matchIndex;
    private boolean replicationPending;

    MemberReplicationContext(int leaderId, ClusterMember targetMember, Agent agent, ConnectionManager connectionManager, State state) {
        this.leaderId = leaderId;
        this.targetMember = targetMember;
        this.agent = agent;
        this.connectionManager = connectionManager;
        this.state = state;
        this.matchListeners = new PriorityQueue<>(comparing(MatchListener::index));
        this.nextIndex = this.state.getLog().getLastIndex() + 1;
    }

    public CompletionStage<Void> replicateEntry(int index) {
        return this.agent.ask(() -> {
            if (index <= this.matchIndex) {
                // Although this shouldn't happen, just in case
                return CompletableFuture.<Void>completedFuture(null);
            }
            CompletableFuture<Void> future = new CompletableFuture<>();
            this.matchListeners.add(new MatchListener(index, future));
            replicate();
            return future;
        }).thenCompose(Function.identity());
    }

    /**
     * Replicate entries (if needed)
     */
    private void replicate() {
        if (this.replicationPending) {
            return;
        }
        Log log = this.state.getLog();
        int lastIndex = log.getLastIndex();
        if (this.nextIndex > lastIndex) {
            return;
        }
        this.replicationPending = true;
        int prevIndex = this.nextIndex - 1;
        LogEntry prevEntry = log.getEntry(prevIndex);
        this.connectionManager.send(this.targetMember, AppendEntries.builder()
                .term(this.state.getCurrentTerm())
                .leaderId(this.leaderId)
                .prevLogIndex(prevIndex)
                .prevLogTerm(prevEntry != null ? prevEntry.getTerm() : 0)
                .entries(log.getEntries(this.nextIndex))
                .leaderCommit(log.getCommitIndex())
                .build()
        ).whenComplete(((response, throwable) -> {
            this.agent.run(() -> {
                this.replicationPending = false;
                if (throwable != null) {
                    // TODO
                } else if (response.isSuccess()) {
                    onMatch(lastIndex);
                } else {
                    onMismatch();
                }
            });
        }));
    }

    /**
     * Remote server's log matches ours.
     * Update the indices we hold for it
     *
     * @param lastIndex the last index that was sent
     */
    private void onMatch(int lastIndex) {
        this.matchIndex = lastIndex;
        this.nextIndex = lastIndex + 1;
        notifyListeners();
        replicate();
    }

    /**
     * Remote server's log doesn't match ours.
     * Retry by going one entry in the past to see if it now matches.
     */
    private void onMismatch() {
        if (this.nextIndex > 1) {
            this.nextIndex--;
            replicate();
        }
    }

    private void notifyListeners() {
        MatchListener listener = this.matchListeners.peek();
        while (listener != null) {
            if (listener.index > this.matchIndex) {
                // No further listeners can trigger
                return;
            }
            this.matchListeners.poll();
            listener.future.complete(null);
            listener = this.matchListeners.peek();
        }
    }

    private record MatchListener(int index, CompletableFuture<Void> future) {
    }
}
