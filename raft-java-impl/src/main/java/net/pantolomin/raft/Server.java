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
    private final AtomicReference<ServerState> serverState = new AtomicReference<>(ServerState.STARTING);
    private final Cluster cluster;
    private final ClusterMember member;
    private final Config config;
    private final StateManager stateManager;
    private final ConnectionManager connectionManager;
    private final Agent agent;
    private final RequestHandlerImpl requestHandler = new RequestHandlerImpl();
    private final State state = new State();
    private ServerBehavior serverBehavior = new StoppedBehavior();

    private Server(@NonNull Cluster cluster, @NonNull ClusterMember member, @NonNull Config config, @NonNull StateManager stateManager, @NonNull ConnectionManager connectionManager, @NonNull Agent agent) {
        this.cluster = cluster;
        this.member = member;
        this.config = config;
        this.stateManager = stateManager;
        this.connectionManager = connectionManager;
        this.agent = agent;
        List<LogEntry> logEntries = this.stateManager.loadState();
        // TODO: init state correctly
        //this.state.setLog(logEntries);
        updateServerState(ServerState.FOLLOWER, new FollowerBehavior());
        this.connectionManager.subscribe(this.requestHandler);
    }

    public void stop() {
        ServerState newState = this.serverState.updateAndGet(currentState -> currentState.isStarted() ? ServerState.STOPPING : currentState);
        if (newState != ServerState.STOPPING) {
            throw new IllegalStateException("Can't stop server - " + newState);
        }
        // TODO: cleanly leave cluster
        this.connectionManager.unsubscribe(this.requestHandler);
        this.agent.stop();
        this.serverState.set(ServerState.STOPPED);
    }

    public CompletionStage<Boolean> add(Object command) {
        return this.agent.ask(() -> this.serverBehavior.add(command)).thenCompose(Function.identity());
    }

    // ************************************************************************
    // ************************************************************************
    // UTILITIES
    // ************************************************************************
    // ************************************************************************

    private void updateServerState(ServerState newState, ServerBehavior newBehavior) {
        this.serverBehavior.stop();
        this.serverState.set(newState);
        this.serverBehavior = newBehavior;
        this.serverBehavior.start();
    }

    private void forEachMember(Consumer<ClusterMember> memberOperation) {
        cluster.getMembers().forEach(m -> {
            if (m != member) {
                memberOperation.accept(m);
            }
        });
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

        void start() {
            this.behaviorActive = true;
        }

        void stop() {
            this.behaviorActive = false;
        }

        abstract AppendEntries.Response handle(ClusterMember member, AppendEntries appendEntries);

        abstract RequestVote.Response handle(ClusterMember member, RequestVote request);

        CompletionStage<Boolean> add(Object command) {
            return CompletableFuture.failedFuture(new IllegalStateException("Can't add entry to the log - server is not the leader"));
        }
    }

    private final class StoppedBehavior extends ServerBehavior {
        AppendEntries.Response handle(ClusterMember member, AppendEntries appendEntries) {
            return new AppendEntries.Response(state.getCurrentTerm(), false);
        }

        @Override
        RequestVote.Response handle(ClusterMember member, RequestVote request) {
            return new RequestVote.Response(state.getCurrentTerm(), false);
        }
    }

    private final class FollowerBehavior extends ServerBehavior {
        private ScheduledFuture<CompletionStage<Void>> electionTimeoutFuture;
        private long electionTimeoutMs;
        private long lastLeaderMessageTimeMs;

        @Override
        public void start() {
            this.lastLeaderMessageTimeMs = System.currentTimeMillis();
            long avgTimeout = config.getElectionTimeoutUnit().toMillis(config.getElectionTimeout());
            this.electionTimeoutMs = avgTimeout + Math.round(2d * Math.random()) - 1000L;
            this.electionTimeoutFuture = agent.schedule(this::onElectionTimeout, this.electionTimeoutMs, TimeUnit.MILLISECONDS);
        }

        @Override
        public void stop() {
            super.stop();
            this.electionTimeoutFuture.cancel(false);
        }

        @Override
        AppendEntries.Response handle(ClusterMember member, AppendEntries appendEntries) {
            // TODO: implement me
            this.lastLeaderMessageTimeMs = System.currentTimeMillis();
            return null;
        }

        @Override
        RequestVote.Response handle(ClusterMember member, RequestVote request) {
            boolean voteGranted = request.getTerm() >= state.getCurrentTerm();
            return new RequestVote.Response(state.getCurrentTerm(), voteGranted);
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
                return;
            }
            // Increment "term" and become candidate
            state.incrementTerm();
            updateServerState(ServerState.CANDIDATE, new CandidateBehavior());
        }
    }

    private final class CandidateBehavior extends ServerBehavior {
        @Override
        public void start() {
            state.becomeCandidate();
            Log entries = state.getLog();
            LogEntry lastEntry = entries.getLast();
            RequestVote voteRequest = RequestVote.builder()
                    .term(state.getCurrentTerm())
                    .candidateId(member.getId())
                    .lastLogIndex(entries.getLastIndex())
                    .lastLogTerm(lastEntry.getTerm())
                    .build();
            forEachMember(targetMember -> requestVote(targetMember, voteRequest));
        }

        @Override
        AppendEntries.Response handle(ClusterMember member, AppendEntries appendEntries) {
            if (appendEntries.getTerm() < state.getCurrentTerm()) {
                // Not recognised as a legitimate leader
                return new AppendEntries.Response(state.getCurrentTerm(), false);
            }
            updateServerState(ServerState.FOLLOWER, new FollowerBehavior());
            // TODO: handle entries (probably updates our term)
            return new AppendEntries.Response(state.getCurrentTerm(), true);
        }

        @Override
        RequestVote.Response handle(ClusterMember member, RequestVote request) {
            if (request.getTerm() > state.getCurrentTerm()) {
                // Higher term, vote for them and move to follower
                updateServerState(ServerState.FOLLOWER, new FollowerBehavior());
                return new RequestVote.Response(state.getCurrentTerm(), true);
            }
            return new RequestVote.Response(state.getCurrentTerm(), false);
        }

        private void requestVote(ClusterMember targetMember, RequestVote voteRequest) {
            connectionManager.send(targetMember, voteRequest).whenComplete((response, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to request vote from member {}", targetMember, throwable);
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
                int votes = state.addVote();
                // TODO: check if enough votes to become leader
            }
        }
    }

    private final class LeaderBehavior extends ServerBehavior {
        private static final LogEntry[] EMPTY_ENTRIES = new LogEntry[0];
        private ScheduledFuture<?> heartbeatsFuture;

        @Override
        public void start() {
            this.heartbeatsFuture = agent.schedule(this::sendHeartbeat, config.getHeartbeatInterval(), config.getHeartbeatInterval(), config.getHeartbeatUnit());
            sendHeartbeat();
        }

        @Override
        public void stop() {
            this.heartbeatsFuture.cancel(false);
        }

        @Override
        AppendEntries.Response handle(ClusterMember member, AppendEntries appendEntries) {
            // TODO: implement me
            return null;
        }

        @Override
        RequestVote.Response handle(ClusterMember member, RequestVote request) {
            if (request.getTerm() > state.getCurrentTerm()) {
                // Higher term, vote for them and move to follower
                updateServerState(ServerState.FOLLOWER, new FollowerBehavior());
                return new RequestVote.Response(state.getCurrentTerm(), true);
            }
            return new RequestVote.Response(state.getCurrentTerm(), false);
        }

        @Override
        public CompletionStage<Boolean> add(Object command) {
            LogEntry entry = new LogEntry(state.getCurrentTerm(), state.getLastApplied());
            state.add(entry);
            // TODO: fill prevLog fields
            AppendEntries heartbeatMessage = AppendEntries.builder()
                    .term(state.getCurrentTerm())
                    .leaderId(member.getId())
                    .prevLogIndex(0)
                    .prevLogTerm(0)
                    .entries(new LogEntry[]{entry})
                    .leaderCommit(state.getCommitIndex())
                    .build();
            forEachMember(targetMember -> sendHeartbeat(targetMember, heartbeatMessage));
            return CompletableFuture.completedFuture(Boolean.FALSE);
        }

        private void sendHeartbeat() {
            // TODO: fill prevLog fields
            AppendEntries heartbeatMessage = AppendEntries.builder()
                    .term(state.getCurrentTerm())
                    .leaderId(member.getId())
                    .prevLogIndex(0)
                    .prevLogTerm(0)
                    .entries(EMPTY_ENTRIES)
                    .leaderCommit(state.getCommitIndex())
                    .build();
            forEachMember(targetMember -> sendHeartbeat(targetMember, heartbeatMessage));
        }

        private void sendHeartbeat(ClusterMember targetMember, AppendEntries heartbeatMessage) {
            connectionManager.send(targetMember, heartbeatMessage).whenComplete(((response, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to send heartbeat to member {}", targetMember, throwable);
                } else {
                    agent.run(() -> onHeartbeatResponse(targetMember, response));
                }
            }));
        }

        private void onHeartbeatResponse(ClusterMember targetMember, AppendEntries.Response response) {
            if (!isBehaviorActive()) {
                // Possible race condition
                // "heartbeats" were sent when was still leader
                return;
            }
            // TODO: update state for remote
            state.getLog();
        }
    }
}
