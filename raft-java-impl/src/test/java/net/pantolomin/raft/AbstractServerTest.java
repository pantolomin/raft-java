package net.pantolomin.raft;

import lombok.experimental.Delegate;
import net.pantolomin.raft.steps.ServerSteps;
import org.junit.After;

public class AbstractServerTest {
    @Delegate
    private final ServerSteps serverSteps = new ServerSteps();

    @After
    public void after() {
        this.serverSteps.clean();
    }
}
