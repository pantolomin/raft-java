package net.pantolomin.raft.steps;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.pantolomin.raft.*;
import net.pantolomin.raft.api.ClusterMember;
import net.pantolomin.raft.api.StateManager;
import net.pantolomin.raft.api.StateManagerImpl;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static net.pantolomin.raft.Utils.get;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotEquals;

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
                .mapToObj(id -> new ClusterMember(id, "host" + id, 5700))
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
        private final ConnectionManagerImpl connectionManager;
        private final Agent agent;
        private Config config;
        private StateManager stateManager;
        private Server server;

        private ServerContext(ClusterMember member) {
            this.member = member;
            this.connectionManager = new ConnectionManagerImpl(this.member, m -> givenServer(m.getId()).getConnectionManager());
            this.agent = new Agent();
            this.config = Config.builder().build();
            this.stateManager = new StateManagerImpl();
        }

        public ServerContext withConfig(Config config) {
            this.config = config;
            return this;
        }

        public ServerContext withStateManager(StateManager stateManager) {
            this.stateManager = stateManager;
            return this;
        }

        public void whenStart() {
            log.info("When server {} starts", this.member.getId());
            this.server = new Server(cluster, this.member, this.config, this.stateManager, this.connectionManager, this.agent);
        }

        public void whenStop() {
            log.info("When server {} stops", this.member.getId());
            if (this.server != null) {
                get(this.server.stop());
                this.server = null;
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
    }
}
