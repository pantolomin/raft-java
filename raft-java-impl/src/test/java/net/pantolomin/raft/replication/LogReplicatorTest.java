package net.pantolomin.raft.replication;

import lombok.extern.slf4j.Slf4j;
import net.pantolomin.raft.Agent;
import net.pantolomin.raft.Config;
import net.pantolomin.raft.api.ConnectionManagerImpl;
import net.pantolomin.raft.domain.*;
import net.pantolomin.raft.log.RaftLogMemory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static net.pantolomin.raft.FutureUtils.get;
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
        this.agent = new Agent(cluster.getMembers()[0]);
    }

    @After
    public void after() {
        if (this.replicator != null) {
            this.replicator.stop();
            this.replicator = null;
        }
        if (this.agent != null) {
            this.agent.stop();
            this.agent = null;
        }
        this.connectionManager.clear();
    }

    @Test
    public void testNormalUseCase() {
        // Create replicator and commit initial message on each remote server
        givenLogReplicator(5_000L, 1, 0);
        thenRemoteMembersReceived(1, 0, 0, 0, 0);

        // Add 2 entries
        CompletionStage<Integer> entryCommitted1 = whenAddEntry();
        CompletionStage<Integer> entryCommitted2 = whenAddEntry();
        CompletionStage<Integer> entryCommitted3 = whenAddEntry();
        // Only the first one got replicated on remotes ...
        thenRemoteMembersReceived(1, 0, 0, 1, 0);
        assertEquals(1, get(entryCommitted1).intValue());
        // ... and the others got replicated together once the leader got the answers
        thenRemoteMembersReceived(1, 1, 1, 2, 1);
        assertEquals(2, get(entryCommitted2).intValue());
        assertEquals(3, get(entryCommitted3).intValue());
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

        CompletionStage<Integer> entryCommitted = whenAddEntry();
        thenRemoteMembersReceived(2, 1, 5, 1, 1);
        assertEquals(6, get(entryCommitted).intValue());
        entryCommitted = whenAddEntry();
        thenRemoteMembersReceived(2, 2, 6, 1, 2);
        assertEquals(7, get(entryCommitted).intValue());
    }

    // ************************************************************************
    // ************************************************************************
    // STEPS
    // ************************************************************************
    // ************************************************************************

    private void givenLogReplicator(long heartbeatIntervalMs, int term, int logEntries) {
        log.info("given LogReplicator with heartbeat interval {} ms, term {} and {} entries", heartbeatIntervalMs, term, logEntries);
        this.config = Config.builder().withHeartbeatInterval(heartbeatIntervalMs, TimeUnit.MILLISECONDS).build();
        this.state = new State(new RaftLogMemory(), 0);
        for (int i = 0; i < logEntries; i++) {
            this.state.getRaftLog().add(new LogEntry(term - 1, new Command()));
        }
        this.state.setCurrentTerm(term);
        this.replicator = get(this.agent.ask(
                () -> new LogReplicator(this.config, this.cluster, this.cluster.getMembers()[LEADER_ID], this.agent, this.connectionManager, this.state)
        ));
        this.replicator.start();
    }

    private CompletionStage<Integer> whenAddEntry() {
        log.info("when adding entry");
        return get(this.agent.ask(() -> {
            this.state.getRaftLog().add(new LogEntry(this.state.getCurrentTerm(), new Command()));
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

    private static final class Command {
        // nothing needed
    }
}
