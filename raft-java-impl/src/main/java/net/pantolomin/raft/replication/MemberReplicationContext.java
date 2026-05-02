package net.pantolomin.raft.replication;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.pantolomin.raft.Agent;
import net.pantolomin.raft.Config;
import net.pantolomin.raft.api.ConnectionManager;
import net.pantolomin.raft.api.RaftLog;
import net.pantolomin.raft.domain.*;

import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.util.Comparator.comparing;
import static net.pantolomin.raft.FutureUtils.unwrap;

@Slf4j
final class MemberReplicationContext {
    private final int memberId;
    @Getter
    private final ClusterMember targetMember;
    private final Agent agent;
    private final ConnectionManager connectionManager;
    private final State state;
    private final Queue<MatchListener> matchListeners;
    private final long heartbeatIntervalMs;

    private int nextIndex;
    private int matchIndex;
    private boolean replicationPending;
    private long lastSentMessageTime;
    private ScheduledFuture<?> heartbeatsFuture;

    public MemberReplicationContext(int memberId, Config config, ClusterMember targetMember, Agent agent, ConnectionManager connectionManager, State state) {
        this.memberId = memberId;
        this.targetMember = targetMember;
        this.agent = agent;
        this.connectionManager = connectionManager;
        this.state = state;
        this.matchListeners = new PriorityQueue<>(comparing(MatchListener::index));
        this.heartbeatIntervalMs = config.getHeartbeatUnit().toMillis(config.getHeartbeatInterval());
        this.nextIndex = this.state.getRaftLog().getLastIndex() + 1;
    }

    /**
     * Start the replication for remote member
     */
    public void start() {
        sendHeartbeat();
    }

    /**
     * Stop the replication for remote member
     */
    public void stop() {
        if (this.heartbeatsFuture != null) {
            this.heartbeatsFuture.cancel(false);
        }
    }

    /**
     * Replicate up to a given index
     *
     * @param index the index
     * @return once the remote server agreed for the index
     */
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
        RaftLog raftLog = this.state.getRaftLog();
        int lastIndex = raftLog.getLastIndex();
        if (this.nextIndex <= lastIndex) {
            sendAppendEntries();
        }
    }

    /**
     * Send a heartbeat if needed. This will send all unmatched entries to the remote member.
     */
    private void sendHeartbeat() {
        long timeTillNextHeartbeatMs = this.lastSentMessageTime + this.heartbeatIntervalMs - System.currentTimeMillis();
        if (timeTillNextHeartbeatMs > 0L) {
            this.heartbeatsFuture = agent.schedule(this::sendHeartbeat, timeTillNextHeartbeatMs, TimeUnit.MILLISECONDS);
        } else {
            sendAppendEntries();
            this.heartbeatsFuture = agent.schedule(this::sendHeartbeat, this.heartbeatIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Sends a AppendEntries message to the remote member. If the remote member is up-to-date, a heartbeat is sent.
     */
    private void sendAppendEntries() {
        this.replicationPending = true;
        RaftLog raftLog = this.state.getRaftLog();
        int lastIndex = raftLog.getLastIndex();
        int prevIndex = this.nextIndex - 1;
        LogEntry prevEntry = raftLog.getEntry(prevIndex);
        AppendEntries appendEntries = AppendEntries.builder()
                .term(this.state.getCurrentTerm())
                .leaderId(this.memberId)
                .prevLogIndex(prevIndex)
                .prevLogTerm(prevEntry != null ? prevEntry.term() : 0)
                .entries(raftLog.getEntries(this.nextIndex))
                .leaderCommit(raftLog.getCommitIndex())
                .build();
        this.connectionManager.send(this.targetMember, appendEntries)
                .whenComplete((response, throwable) -> this.agent.run(() -> {
                    this.replicationPending = false;
                    if (throwable != null) {
                        if (!(unwrap(throwable) instanceof NotConnectedException)) {
                            log.error("[{}] Failed to send entries to member {}", memberId, targetMember, throwable);
                        }
                    } else if (response.success()) {
                        onMatch(lastIndex);
                    } else {
                        onMismatch();
                    }
                }));
        this.lastSentMessageTime = System.currentTimeMillis();
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

    /**
     * Notify listeners once their index is matched
     */
    private void notifyListeners() {
        MatchListener listener = this.matchListeners.peek();
        while (listener != null) {
            if (listener.index() > this.matchIndex) {
                // No further listeners can trigger
                return;
            }
            this.matchListeners.poll();
            listener.future().complete(null);
            listener = this.matchListeners.peek();
        }
    }

    private record MatchListener(int index, CompletableFuture<Void> future) {
        // nothing else needed
    }
}
