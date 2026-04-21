package net.pantolomin.raft;

import net.pantolomin.raft.steps.ServerSteps;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static net.pantolomin.raft.Utils.sleep;

public class ServerTest extends AbstractServerTest {
    @Test
    public void testNoLeader() {
        givenCluster(5);
        ServerSteps.ServerContext server0 = givenServer(0)
                .withConfig(Config.builder().withElectionTimeout(1L, TimeUnit.SECONDS).build());
        ServerSteps.ServerContext server1 = givenServer(1)
                .withConfig(Config.builder().withElectionTimeout(1L, TimeUnit.SECONDS).build());
        server0.whenStart();
        server1.whenStart();

        // Both become CANDIDATE ...
        server0.thenStateIs(ServerState.CANDIDATE);
        server1.thenStateIs(ServerState.CANDIDATE);

        // ... but no-one becomes LEADER
        sleep(100L);
        server0.thenStateIsNot(ServerState.LEADER);
        server1.thenStateIsNot(ServerState.LEADER);
    }

    @Test
    public void testLeader() {
        givenCluster(5);
        ServerSteps.ServerContext server0 = givenServer(0)
                .withConfig(Config.builder().withElectionTimeout(1L, TimeUnit.SECONDS).build());
        ServerSteps.ServerContext server1 = givenServer(1)
                .withConfig(Config.builder().withElectionTimeout(1L, TimeUnit.SECONDS).build());
        ServerSteps.ServerContext server2 = givenServer(2)
                .withConfig(Config.builder().withElectionTimeout(1L, TimeUnit.SECONDS).build());
        ServerSteps.ServerContext server3 = givenServer(3)
                .withConfig(Config.builder().withElectionTimeout(500L, TimeUnit.MILLISECONDS).build());
        server0.whenStart();
        server1.whenStart();
        server2.whenStart();
        server3.whenStart();

        // We have a LEADER
        server3.thenStateIs(ServerState.LEADER);
    }
}
