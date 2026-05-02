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
                .withConfig(Config.builder().withElectionTimeout(500L, TimeUnit.MILLISECONDS).build());
        ServerSteps.ServerContext server1 = givenServer(1)
                .withConfig(Config.builder().withElectionTimeout(1L, TimeUnit.SECONDS).build());
        server0.whenStart();
        server1.whenStart();

        // One becomes CANDIDATE ...
        server0.thenStateIs(ServerState.CANDIDATE);
        server1.thenStateIs(ServerState.FOLLOWER);

        // ... but no-one becomes LEADER
        sleep(100L);
        server0.thenStateIsNot(ServerState.LEADER);
        server1.thenStateIsNot(ServerState.LEADER);
    }

    @Test
    public void testLeader() {
        givenCluster(5);
        ServerSteps.ServerContext server0 = givenServer(0)
                .withConfig(Config.builder().withElectionTimeout(2L, TimeUnit.SECONDS).build());
        ServerSteps.ServerContext server1 = givenServer(1)
                .withConfig(Config.builder().withElectionTimeout(500L, TimeUnit.MILLISECONDS).build());
        ServerSteps.ServerContext server2 = givenServer(2)
                .withConfig(Config.builder().withElectionTimeout(2L, TimeUnit.SECONDS).build());
        ServerSteps.ServerContext server3 = givenServer(3)
                .withConfig(Config.builder().withElectionTimeout(2L, TimeUnit.SECONDS).build());
        server0.whenStart();
        server1.whenStart();
        server2.whenStart();
        server3.whenStart();

        // We have a LEADER
        server1.thenStateIs(ServerState.LEADER);
    }

    @Test
    public void testFigure7() {
        givenCluster(7);
        ServerSteps.ServerContext server0 = server(0, 5000L);
        ServerSteps.ServerContext server1 = server(1, 20000L);
        ServerSteps.ServerContext server2 = server(2, 20000L);
        ServerSteps.ServerContext server3 = server(3, 20000L);
        ServerSteps.ServerContext server4 = server(4, 20000L);
        ServerSteps.ServerContext server5 = server(5, 20000L);
        ServerSteps.ServerContext server6 = server(6, 20000L);
        server0.whenStart();
        server1.whenStart();
        server2.whenStart();
        server3.whenStart();
        server4.whenStart();
        server5.whenStart();
        server6.whenStart();

        // We have a LEADER
        server0.thenStateIs(ServerState.LEADER);
        ServerSteps.Command cmd1 = server0.whenAdd(1);
        ServerSteps.Command cmd2 = server0.whenAdd(2);
        ServerSteps.Command cmd3 = server0.whenAdd(3);
        cmd1.thenSuccess();
        cmd2.thenSuccess();
        cmd3.thenSuccess();
        server0.whenStop();
        server0.thenEntries(1, 1, 1);
        server1.thenEntries(1, 1, 1);
        server2.thenEntries(1, 1, 1);
        server3.thenEntries(1, 1, 1);
        server4.thenEntries(1, 1, 1);
        server5.thenEntries(1, 1, 1);
        server6.thenEntries(1, 1, 1);

        // We have a new LEADER
        server6.thenStateIs(ServerState.LEADER);
        server1.whenStop();
        server2.whenStop();
        server3.whenStop();
        server4.whenStop();
        server5.whenStop();
        cmd1 = server6.whenAdd(1);
        cmd2 = server6.whenAdd(2);
        cmd3 = server6.whenAdd(3);
        cmd1.thenNoAnswer();
        cmd2.thenNoAnswer();
        cmd3.thenNoAnswer();
        server6.whenStop();
        server1.whenStart();
        server2.whenStart();
        server4.whenStart();
        server6.whenStart();
        server0.thenEntries(1, 1, 1);
        server1.thenEntries(1, 1, 1, 2, 2, 2);
        server2.thenEntries(1, 1, 1, 2, 2, 2);
        server3.thenEntries(1, 1, 1);
        server4.thenEntries(1, 1, 1, 2, 2, 2);
        server5.thenEntries(1, 1, 1);
        server6.thenEntries(1, 1, 1, 2, 2, 2);

        // We have a new LEADER
        server6.thenStateIs(ServerState.LEADER);
        server1.whenStop();
        server2.whenStop();
        server4.whenStop();
        cmd1 = server6.whenAdd(1);
        cmd2 = server6.whenAdd(2);
        cmd3 = server6.whenAdd(3);
        ServerSteps.Command cmd4 = server6.whenAdd(4);
        ServerSteps.Command cmd5 = server6.whenAdd(5);
        cmd1.thenNoAnswer();
        cmd2.thenNoAnswer();
        cmd3.thenNoAnswer();
        cmd4.thenNoAnswer();
        cmd5.thenNoAnswer();
        server6.whenStop();
        server1.whenStart();
        server2.whenStart();
        server3.whenStart();
        server4.whenStart();
        server(5, 200L).whenStart();
        server0.thenEntries(1, 1, 1);
        server1.thenEntries(1, 1, 1, 2, 2, 2);
        server2.thenEntries(1, 1, 1, 2, 2, 2);
        server3.thenEntries(1, 1, 1);
        server4.thenEntries(1, 1, 1, 2, 2, 2);
        server5.thenEntries(1, 1, 1);
        server6.thenEntries(1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3);

        // We have a new LEADER
        server5.thenStateIs(ServerState.LEADER);
        server0.whenStart();
        server5.whenAdd(1).thenSuccess();
        server2.whenStop();
        server5.whenAdd(2).thenSuccess();
        server0.whenStop();
        server1.whenStop();
        server3.whenStop();
        server4.whenStop();
        cmd1 = server5.whenAdd(3);
        cmd2 = server5.whenAdd(4);
        cmd1.thenNoAnswer();
        cmd2.thenNoAnswer();
        server5.whenStop();
        server0.whenStart();
        server1.whenStart();
        server3.whenStart();
        server4.whenStart();
        server0.thenEntries(1, 1, 1, 4, 4);
        server1.thenEntries(1, 1, 1, 4, 4);
        server2.thenEntries(1, 1, 1, 4);
        server3.thenEntries(1, 1, 1, 4, 4);
        server4.thenEntries(1, 1, 1, 4, 4);
        server5.thenEntries(1, 1, 1, 4, 4, 4, 4);
        server6.thenEntries(1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3);

        // We have a new LEADER
        server0.thenStateIs(ServerState.LEADER);
        server0.whenAdd(1).thenSuccess();
        server0.whenAdd(2).thenSuccess();
        server0.whenStop();

        // We have a new LEADER
        server3.thenStateIs(ServerState.LEADER);
        server0.whenStart();
        server3.whenAdd(1).thenSuccess();
        server3.whenAdd(2).thenSuccess();
        server1.whenStop();
        cmd1 = server3.whenAdd(3);
        server0.whenStop();
        server4.whenStop();
        cmd2 = server3.whenAdd(4);
        cmd1.thenNoAnswer();
        cmd2.thenNoAnswer();
        server3.whenStop();
        server1.whenStart();
        server2.whenStart();
        server5.whenStart();
        server(4, 200L).whenStart();

        // We have a new LEADER
        server4.thenStateIs(ServerState.LEADER);
        server1.whenStop();
        server2.whenStop();
        server5.whenStop();
        cmd1 = server4.whenAdd(1);
        cmd2 = server4.whenAdd(2);
        cmd1.thenNoAnswer();
        cmd2.thenNoAnswer();
        server4.whenStop();

        // Check that we have the entries from figure 7
        server0.thenEntries(1, 1, 1, 4, 4, 5, 5, 6, 6, 6);
        server1.thenEntries(1, 1, 1, 4, 4, 5, 5, 6, 6);
        server2.thenEntries(1, 1, 1, 4);
        server3.thenEntries(1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6);
        server4.thenEntries(1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7);
        server5.thenEntries(1, 1, 1, 4, 4, 4, 4);
        server6.thenEntries(1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3);
    }

    private ServerSteps.ServerContext server(int memberId, long electionTimeout) {
        return givenServer(memberId)
                .withConfig(Config.builder()
                        .withHeartbeatInterval(50L, TimeUnit.MILLISECONDS)
                        .withElectionTimeout(electionTimeout, TimeUnit.MILLISECONDS)
                        .build());
    }
}
