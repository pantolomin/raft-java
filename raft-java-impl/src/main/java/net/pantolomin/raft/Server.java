package net.pantolomin.raft;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import net.pantolomin.raft.api.ClusterMember;
import net.pantolomin.raft.api.ConnectionManager;
import net.pantolomin.raft.api.RequestHandler;
import net.pantolomin.raft.api.StateManager;
import net.pantolomin.raft.domain.AppendEntries;
import net.pantolomin.raft.domain.LogEntry;
import net.pantolomin.raft.domain.RequestVote;
import net.pantolomin.raft.domain.State;
import net.pantolomin.raft.replication.LogReplicator;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
public class Server {
    private final AtomicReference<ServerState> serverState;
    private final Cluster cluster;
    private final ClusterMember localMember;
    private final Config config;
    private final StateManager stateManager;
    private final ConnectionManager connectionManager;
    private final Agent agent;
    private final RequestHandlerImpl requestHandler;
    private final State state;
    private final LogReplicator logReplicator;
    private ServerBehavior serverBehavior;
    private ClusterMember lastKnownLeader;

    public Server(@NonNull Cluster cluster, @NonNull ClusterMember localMember, @NonNull Config config, @NonNull StateManager stateManager, @NonNull ConnectionManager connectionManager, @NonNull Agent agent) {
        this.serverState = new AtomicReference<>(ServerState.STARTING);
        this.cluster = cluster;
        this.localMember = localMember;
        this.config = config;
        this.stateManager = stateManager;
        this.connectionManager = connectionManager;
        this.agent = agent;
        this.requestHandler = new RequestHandlerImpl();
        this.state = new State();
        this.serverBehavior = new StoppedBehavior();
        this.logReplicator = new LogReplicator(config, cluster, localMember, agent, connectionManager, this.state);
        List<LogEntry> logEntries = this.stateManager.loadState();
        logEntries.forEach(this.state.getLog()::add);
        this.connectionManager.subscribe(this.requestHandler);
        this.agent.run(() -> updateServerState(ServerState.FOLLOWER, new FollowerBehavior()));
    }

    public CompletionStage<Void> stop() {
        return this.agent.run(() -> {
            ServerState currentState = this.serverState.get();
            if (!currentState.isStarted()) {
                throw new IllegalStateException("Can't stop server - " + currentState);
            }
            updateServerState(ServerState.STOPPING, new StoppedBehavior());
            this.connectionManager.unsubscribe(this.requestHandler);
            this.agent.stop();
            log.info("[{}] State change: {} -> {}", localMember.getId(), ServerState.STOPPING, ServerState.STOPPED);
            this.serverState.set(ServerState.STOPPED);
        });
    }

    public ServerState getState() {
        return this.serverState.get();
    }

    /**
     * Adds a command to the log
     *
     * @param command the command
     * @return the result for the command
     */
    public CompletionStage<CommandResult> add(Object command) {
        return this.agent.ask(() -> this.serverBehavior.add(command)).thenCompose(Function.identity());
    }

    // ************************************************************************
    // ************************************************************************
    // UTILITIES
    // ************************************************************************
    // ************************************************************************

    private <S extends ServerBehavior> S updateServerState(ServerState newState, S newBehavior) {
        ServerState oldState = this.serverState.getAndSet(newState);
        log.info("[{}] State change: {} -> {}", localMember.getId(), oldState, newState);
        this.serverBehavior.stop();
        this.serverBehavior = newBehavior;
        this.serverBehavior.start();
        return newBehavior;
    }

    private void forEachMember(Consumer<ClusterMember> memberOperation) {
        ClusterMember[] clusterMembers = cluster.getMembers();
        for (ClusterMember m : clusterMembers) {
            if (m != localMember) {
                memberOperation.accept(m);
            }
        }
    }

    private AppendEntries.Response handleAppendEntries(AppendEntries appendEntries) {
        int remoteTerm = appendEntries.getTerm();
        Log entries = state.getLog();
        LogEntry prevEntry = entries.getEntry(appendEntries.getPrevLogIndex());
        if (prevEntry == null || prevEntry.getTerm() != appendEntries.getPrevLogTerm()) {
            if (prevEntry == null) {
                prevEntry = entries.getLast();
            }
            return new AppendEntries.Response(prevEntry != null ? prevEntry.getTerm() : 0, false);
        }
        entries.add(appendEntries.getPrevLogIndex(), appendEntries.getEntries());
        entries.commit(appendEntries.getLeaderCommit());
        state.setCurrentTerm(remoteTerm);
        return new AppendEntries.Response(remoteTerm, true);
    }

    // ************************************************************************
    // ************************************************************************
    // REQUEST HANDLER
    // ************************************************************************
    // ************************************************************************

    private final class RequestHandlerImpl implements RequestHandler {
        @Override
        public CompletionStage<AppendEntries.Response> handle(ClusterMember member, AppendEntries appendEntries) {
            return agent.ask(() -> serverBehavior.handle(member, appendEntries));
        }

        @Override
        public CompletionStage<RequestVote.Response> handle(ClusterMember member, RequestVote request) {
            return agent.ask(() -> serverBehavior.handle(member, request));
        }
    }

    // ************************************************************************
    // ************************************************************************
    // BEHAVIORS
    // ************************************************************************
    // ************************************************************************

    @Getter
    private abstract class ServerBehavior {
        private boolean behaviorActive;

        final void start() {
            log.debug("[{}] Behavior starting: {}", localMember.getId(), getClass().getSimpleName());
            onStart();
            this.behaviorActive = true;
            log.debug("[{}] Behavior started: {}", localMember.getId(), getClass().getSimpleName());
        }

        final void stop() {
            log.debug("[{}] Behavior stopping: {}", localMember.getId(), getClass().getSimpleName());
            this.behaviorActive = false;
            onStop();
            log.debug("[{}] Behavior stopped: {}", localMember.getId(), getClass().getSimpleName());
        }

        protected void onStart() {
            // nothing to do
        }

        protected void onStop() {
            // nothing to do
        }

        abstract AppendEntries.Response handle(ClusterMember member, AppendEntries appendEntries);

        abstract RequestVote.Response handle(ClusterMember member, RequestVote request);

        CompletionStage<CommandResult> add(Object command) {
            return CompletableFuture.completedFuture(CommandResult.notLeader(lastKnownLeader));
        }
    }

    private final class StoppedBehavior extends ServerBehavior {
        AppendEntries.Response handle(ClusterMember member, AppendEntries appendEntries) {
            throw new IllegalStateException("Server stopped");
        }

        @Override
        RequestVote.Response handle(ClusterMember member, RequestVote request) {
            throw new IllegalStateException("Server stopped");
        }
    }

    private final class FollowerBehavior extends ServerBehavior {
        private ScheduledFuture<CompletionStage<Void>> electionTimeoutFuture;
        private long electionTimeoutMs;
        private long lastLeaderMessageTimeMs;

        @Override
        protected void onStart() {
            this.lastLeaderMessageTimeMs = System.currentTimeMillis();
            long avgTimeout = config.getElectionTimeoutUnit().toMillis(config.getElectionTimeout());
            this.electionTimeoutMs = Math.round(0.8d * avgTimeout + (0.4d * avgTimeout * Math.random()));
            log.info("[{}] Election timeout set to: {} ms", localMember.getId(), this.electionTimeoutMs);
            this.electionTimeoutFuture = agent.schedule(this::onElectionTimeout, this.electionTimeoutMs, TimeUnit.MILLISECONDS);
        }

        @Override
        protected void onStop() {
            this.electionTimeoutFuture.cancel(false);
        }

        @Override
        AppendEntries.Response handle(ClusterMember member, AppendEntries appendEntries) {
            this.lastLeaderMessageTimeMs = System.currentTimeMillis();
            lastKnownLeader = member;
            return handleAppendEntries(appendEntries);
        }

        @Override
        RequestVote.Response handle(ClusterMember member, RequestVote request) {
            int requestTerm = request.getTerm();
            if (requestTerm > state.getCurrentTerm()) {
                log.info("[{}] Accepting candidate {} (term {})", localMember.getId(), member.getId(), requestTerm);
                this.lastLeaderMessageTimeMs = System.currentTimeMillis();
                state.setCurrentTerm(requestTerm);
                return new RequestVote.Response(requestTerm, true);
            }
            log.info("[{}] Rejecting candidate {} (term {})", localMember.getId(), member.getId(), requestTerm);
            return new RequestVote.Response(state.getCurrentTerm(), false);
        }

        private void onElectionTimeout() {
            if (!isBehaviorActive()) {
                // Possible (although highly improbable) race condition
                // "election timeout" task got scheduled just before being cancelled
                return;
            }
            long currentTime = System.currentTimeMillis();
            long timeoutTime = this.lastLeaderMessageTimeMs + this.electionTimeoutMs;
            if (currentTime < timeoutTime) {
                // Messages were received in the meantime, reschedule election timeout
                this.electionTimeoutFuture = agent.schedule(this::onElectionTimeout, timeoutTime - currentTime, TimeUnit.MILLISECONDS);
            } else {
                log.info("[{}] Election timeout", localMember.getId());
                updateServerState(ServerState.CANDIDATE, new CandidateBehavior());
            }
        }
    }

    private final class CandidateBehavior extends ServerBehavior {
        private ScheduledFuture<CompletionStage<Void>> electionTimeoutFuture;
        private int votedFor;

        @Override
        protected void onStart() {
            int newTerm = state.getCurrentTerm() + 1;
            state.setCurrentTerm(newTerm);
            this.votedFor = 1;
            Log entries = state.getLog();
            LogEntry lastEntry = entries.getLast();
            RequestVote voteRequest = RequestVote.builder()
                    .term(newTerm)
                    .candidateId(localMember.getId())
                    .lastLogIndex(entries.getLastIndex())
                    .lastLogTerm(lastEntry != null ? lastEntry.getTerm() : 0)
                    .build();
            long electionTimeoutMs = config.getElectionTimeoutUnit().toMillis(config.getElectionTimeout());
            log.info("[{}] Request for votes (term {}) - timeout: {} ms", localMember.getId(), newTerm, electionTimeoutMs);
            forEachMember(targetMember -> requestVote(targetMember, voteRequest));
            this.electionTimeoutFuture = agent.schedule(this::onElectionTimeout, electionTimeoutMs, TimeUnit.MILLISECONDS);
        }

        @Override
        protected void onStop() {
            this.electionTimeoutFuture.cancel(false);
        }

        @Override
        AppendEntries.Response handle(ClusterMember member, AppendEntries appendEntries) {
            int entriesTerm = appendEntries.getTerm();
            if (entriesTerm < state.getCurrentTerm()) {
                // Not recognised as a legitimate leader
                return new AppendEntries.Response(state.getCurrentTerm(), false);
            }
            log.info("[{}] Received new entry invalidating candidate (term {})", localMember.getId(), entriesTerm);
            return updateServerState(ServerState.FOLLOWER, new FollowerBehavior())
                    .handle(member, appendEntries);
        }

        @Override
        RequestVote.Response handle(ClusterMember member, RequestVote request) {
            int remoteTerm = request.getTerm();
            if (remoteTerm > state.getCurrentTerm()) {
                log.info("[{}] Opponent {} becomes better prospect (term {})", localMember.getId(), member.getId(), remoteTerm);
                return updateServerState(ServerState.FOLLOWER, new FollowerBehavior())
                        .handle(member, request);
            }
            log.info("[{}] Opponent {} rejected (term {})", localMember.getId(), member.getId(), remoteTerm);
            return new RequestVote.Response(state.getCurrentTerm(), false);
        }

        private void requestVote(ClusterMember targetMember, RequestVote voteRequest) {
            connectionManager.send(targetMember, voteRequest).whenComplete((response, throwable) -> {
                if (throwable != null) {
                    log.error("[{}] Failed to request vote from member {}", localMember.getId(), targetMember, throwable);
                } else {
                    agent.run(() -> onVoteResponse(targetMember, response));
                }
            });
        }

        private void onVoteResponse(ClusterMember member, RequestVote.Response response) {
            if (!isBehaviorActive()) {
                // Possible race condition
                // "vote requests" were sent before being judged obsolete
                return;
            }
            if (response.isVoteGranted()) {
                this.votedFor++;
                log.info("[{}] Vote accepted by {} - {} votes for us", localMember.getId(), member.getId(), this.votedFor);
                int majority = cluster.getMembers().length / 2 + 1;
                if (this.votedFor >= majority) {
                    log.info("[{}] Obtained the majority of votes", localMember.getId());
                    updateServerState(ServerState.LEADER, new LeaderBehavior());
                }
            }
        }

        private void onElectionTimeout() {
            if (!isBehaviorActive()) {
                // Possible (although highly improbable) race condition
                // "election timeout" task got scheduled just before being cancelled
                return;
            }
            log.info("[{}] Not enough votes before timeout - retry", localMember.getId());
            onStop();
            onStart();
        }
    }

    private final class LeaderBehavior extends ServerBehavior {
        @Override
        protected void onStart() {
            log.info("[{}] Starting log replication", localMember.getId());
            logReplicator.start();
        }

        @Override
        protected void onStop() {
            logReplicator.stop();
            log.info("[{}] Stopped log replication", localMember.getId());
        }

        @Override
        AppendEntries.Response handle(ClusterMember member, AppendEntries appendEntries) {
            if (appendEntries.getTerm() < state.getCurrentTerm()) {
                // Not recognised as a legitimate leader
                return new AppendEntries.Response(state.getCurrentTerm(), false);
            }
            return updateServerState(ServerState.FOLLOWER, new FollowerBehavior())
                    .handle(member, appendEntries);
        }

        @Override
        RequestVote.Response handle(ClusterMember member, RequestVote request) {
            int remoteTerm = request.getTerm();
            if (remoteTerm > state.getCurrentTerm()) {
                log.info("[{}] Opponent {} becomes better prospect (term {})", localMember.getId(), member.getId(), remoteTerm);
                return updateServerState(ServerState.FOLLOWER, new FollowerBehavior())
                        .handle(member, request);
            }
            return new RequestVote.Response(state.getCurrentTerm(), false);
        }

        @Override
        public CompletionStage<CommandResult> add(Object command) {
            state.getLog().add(new LogEntry(state.getCurrentTerm(), command));
            return logReplicator.replicate().thenApply(commited -> CommandResult.success());
        }
    }
}
