package net.pantolomin.raft.replication;

import lombok.Getter;
import net.pantolomin.raft.Agent;
import net.pantolomin.raft.Log;

import java.util.concurrent.CompletableFuture;

final class EntryReplicationContext {
    private final int lastIndex;
    @Getter
    private final CompletableFuture<Integer> future = new CompletableFuture<>();
    private int neededForCommit;

    EntryReplicationContext(Agent agent, Log log, MemberReplicationContext[] memberReplicationContexts) {
        // Need half + 1 servers to agree (leader already agrees obviously)
        this.neededForCommit = memberReplicationContexts.length / 2;
        this.lastIndex = log.getLastIndex();
        for (MemberReplicationContext replicationContext : memberReplicationContexts) {
            if (replicationContext != null) {
                replicationContext.replicateEntry(this.lastIndex).thenAccept(done -> agent.run(this::onEntryReplicated));
            }
        }
    }

    private void onEntryReplicated() {
        this.neededForCommit--;
        if (this.neededForCommit == 0) {
            this.future.complete(this.lastIndex);
        }
    }
}
