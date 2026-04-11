package net.pantolomin.raft.replication;

import lombok.extern.slf4j.Slf4j;
import net.pantolomin.raft.Agent;
import net.pantolomin.raft.Cluster;
import net.pantolomin.raft.Config;
import net.pantolomin.raft.api.ClusterMember;
import net.pantolomin.raft.api.ConnectionManagerImpl;
import net.pantolomin.raft.domain.AppendEntries;
import net.pantolomin.raft.domain.LogEntry;
import net.pantolomin.raft.domain.State;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;

@Slf4j
public class LogReplicatorTest {
    private static final int LEADER_ID = 0;

    private final Cluster cluster = cluster(0, 1, 2);
    private final ConnectionManagerImpl connectionManager = new ConnectionManagerImpl();
    private Config config;
    private Agent agent;
    private State state;
    private LogReplicator replicator;

    @Before
    public void before() {
        this.agent = new Agent();
    }

    @Test
    public void testNormalUseCase() {
        // Create replicator and commit initial message on each remote server
        givenLogReplicator(5_000L, 1, 0);
        thenRemoteMembersReceived(1, 0, 0, 0, 0);

        // Add 2 entries
        CompletionStage<Void> entryCommitted1 = whenAddEntry();
        CompletionStage<Void> entryCommitted2 = whenAddEntry();
        CompletionStage<Void> entryCommitted3 = whenAddEntry();
        // Only the first one got replicated on remotes ...
        thenRemoteMembersReceived(1, 0, 0, 1, 0);
        get(entryCommitted1);
        // ... and the others got replicated together once the leader got the answers
        thenRemoteMembersReceived(1, 1, 1, 2, 1);
        get(entryCommitted2);
        get(entryCommitted3);
    }

    @Test
    public void testLogAlreadyFilled() {
        // Create replicator and commit initial message on each remote server
        givenLogReplicator(5_000L, 2, 5);
        // Need multiple tries from server to get a match from remotes
        thenRemoteMembersReceived(2, 1, 5, 0, 0);
        thenRemoteMembersReceived(2, 1, 4, 1, 0);
        thenRemoteMembersReceived(2, 1, 3, 2, 0);
        thenRemoteMembersReceived(2, 1, 2, 3, 0);
        thenRemoteMembersReceived(2, 1, 1, 4, 0);
        thenRemoteMembersReceived(2, 0, 0, 5, 0);

        CompletionStage<Void> entryCommitted = whenAddEntry();
        thenRemoteMembersReceived(2, 1, 5, 1, 1);
        get(entryCommitted);
        entryCommitted = whenAddEntry();
        thenRemoteMembersReceived(2, 2, 6, 1, 2);
        get(entryCommitted);
    }

    // ************************************************************************
    // ************************************************************************
    // STEPS
    // ************************************************************************
    // ************************************************************************

    private void givenLogReplicator(long heartbeatIntervalMs, int term, int logEntries) {
        log.info("given LogReplicator with heartbeat interval {} ms, term {} and {} entries", heartbeatIntervalMs, term, logEntries);
        this.config = Config.builder()
                .heartbeatInterval(heartbeatIntervalMs)
                .heartbeatUnit(TimeUnit.MILLISECONDS)
                .build();
        this.state = new State();
        for (int i = 0; i < logEntries; i++) {
            this.state.getLog().add(new LogEntry(term - 1, new Command()));
        }
        this.state.setCurrentTerm(term);
        this.replicator = get(this.agent.ask(
                () -> new LogReplicator(this.config, this.cluster, this.cluster.getMembers()[LEADER_ID], this.agent, this.connectionManager, this.state)
        ));
    }

    private CompletionStage<Void> whenAddEntry() {
        log.info("when adding entry");
        return get(this.agent.ask(() -> {
            this.state.getLog().add(new LogEntry(this.state.getCurrentTerm(), new Command()));
            return this.replicator.replicate();
        }));
    }

    private void thenRemoteMembersReceived(int term, int prevTerm, int prevIndex, int nbEntries, int termAnswer) {
        log.info("then transmitted (term: {}, prevTerm: {}, prevIndex: {}) with {} entries", term, prevTerm, prevIndex, nbEntries);
        for (ClusterMember member : this.cluster.getMembers()) {
            if (member.getId() == LEADER_ID) {
                continue;
            }
            ConnectionManagerImpl.AppendEntriesContext msg = this.connectionManager.thenMemberReceivedAppendEntries(member.getId());
            check(msg, term, prevTerm, prevIndex, nbEntries);
            if (termAnswer == prevTerm) {
                msg.success(termAnswer);
            } else {
                msg.failed(termAnswer);
            }
        }
    }

    // ************************************************************************
    // ************************************************************************
    // UTILS
    // ************************************************************************
    // ************************************************************************

    private Cluster cluster(int... members) {
        return new Cluster(Arrays.stream(members)
                .mapToObj(id -> new ClusterMember(id, "host" + id, 5700))
                .toArray(ClusterMember[]::new)
        );
    }

    private void check(ConnectionManagerImpl.AppendEntriesContext msg, int term, int prevTerm, int prevIndex, int nbEntries) {
        AppendEntries message = msg.getMessage();
        assertEquals(LEADER_ID, message.getLeaderId());
        assertEquals(term, message.getTerm());
        assertEquals(prevTerm, message.getPrevLogTerm());
        assertEquals(prevIndex, message.getPrevLogIndex());
        assertEquals(nbEntries, message.getEntries().length);
    }

    private <T> T get(CompletionStage<T> future) {
        try {
            return future.toCompletableFuture().get(5L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
        } catch (ExecutionException | TimeoutException e) {
            throw new CompletionException(e);
        }
    }

    private static final class Command {
        // nothing needed
    }
}
