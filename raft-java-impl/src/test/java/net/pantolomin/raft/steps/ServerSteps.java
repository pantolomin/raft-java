package net.pantolomin.raft.steps;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.pantolomin.raft.*;
import net.pantolomin.raft.api.ConnectionManager;
import net.pantolomin.raft.api.RaftLog;
import net.pantolomin.raft.domain.Cluster;
import net.pantolomin.raft.domain.ClusterMember;
import net.pantolomin.raft.log.RaftLogMemory;
import net.pantolomin.raft.net.ConnectionManagerNetty;

import java.util.Arrays;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static net.pantolomin.raft.FutureUtils.get;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

@Slf4j
public class ServerSteps {
    private Cluster cluster;
    private ServerContext[] servers;

    public void clean() {
        Arrays.stream(this.servers).forEach(ServerContext::whenStop);
        this.servers = null;
        this.cluster = null;
    }

    public void givenCluster(int nbMembers) {
        log.info("Given cluster with {} members", nbMembers);
        this.cluster = new Cluster(IntStream.range(0, nbMembers)
                .mapToObj(id -> new ClusterMember(id, "127.0.0.1", 5700 + id))
                .toArray(ClusterMember[]::new)
        );
        this.servers = new ServerContext[nbMembers];
        for (int i = 0; i < nbMembers; i++) {
            this.servers[i] = new ServerContext(this.cluster.getMembers()[i]);
        }
    }

    public ServerContext givenServer(int memberId) {
        return this.servers[memberId];
    }

    @Getter
    @Setter
    public final class ServerContext {
        private final ClusterMember member;
        private final ConnectionManager connectionManager;
        private Agent agent;
        private Config config;
        private RaftLog raftLog;
        private Server server;

        private ServerContext(ClusterMember member) {
            this.member = member;
            this.connectionManager = new ConnectionManagerNetty(cluster, this.member);
            this.config = Config.builder().build();
            this.raftLog = new RaftLogMemory();
        }

        public ServerContext withConfig(Config config) {
            this.config = config;
            return this;
        }

        public ServerContext withRaftLog(RaftLog raftLog) {
            this.raftLog = raftLog;
            return this;
        }

        public void whenStart() {
            log.info("When server {} starts", this.member.getId());
            this.agent = new Agent(this.member);
            this.server = new Server(cluster, this.member, this.config, this.raftLog, this.connectionManager, this.agent);
        }

        public void whenStop() {
            log.info("When server {} stops", this.member.getId());
            if (this.server != null) {
                get(this.server.stop());
                this.server = null;
                this.agent = null;
            }
        }

        public void thenStateIs(ServerState state) {
            log.info("Then state is {}", state);
            await().atMost(5L, TimeUnit.SECONDS).until(() -> this.server.getState(), is(state));
        }

        public void thenStateIsNot(ServerState state) {
            log.info("Then state is not {}", state);
            assertNotEquals(state, this.server.getState());
        }

        public Command whenAdd(Object command) {
            log.info("When add command: {}", command);
            return new Command(this.server.add(command));
        }

        public void thenEntries(int... terms) {
            await().atMost(5L, TimeUnit.SECONDS).untilAsserted(
                    () -> this.raftLog.getEntries(0),
                    logEntries -> {
                        assertEquals(terms.length, logEntries.length);
                        for (int i = 0; i < terms.length; i++) {
                            assertEquals(terms[i], logEntries[i].term());
                        }
                    });
        }
    }

    @RequiredArgsConstructor
    public final class Command {
        private final CompletionStage<CommandResult> result;

        public void thenSuccess() {
            assertTrue(get(this.result).isSuccess());
        }

        public void thenNoAnswer() {
            assertFalse(this.result.toCompletableFuture().isDone());
        }
    }
}
