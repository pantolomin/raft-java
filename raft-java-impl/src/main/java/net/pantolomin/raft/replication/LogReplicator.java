package net.pantolomin.raft.replication;

import lombok.RequiredArgsConstructor;
import net.pantolomin.raft.Agent;
import net.pantolomin.raft.Cluster;
import net.pantolomin.raft.api.ClusterMember;
import net.pantolomin.raft.api.ConnectionManager;
import net.pantolomin.raft.domain.State;

import java.util.concurrent.CompletionStage;

@RequiredArgsConstructor
public class LogReplicator {
    private final ClusterMember member;
    private final Agent agent;
    private final ConnectionManager connectionManager;
    private final State state;
    private MemberReplicationContext[] memberReplicationContexts;

    public LogReplicator(Cluster cluster, ClusterMember member, Agent agent, ConnectionManager connectionManager, State state) {
        this.member = member;
        this.agent = agent;
        this.connectionManager = connectionManager;
        this.state = state;
        ClusterMember[] clusterMembers = cluster.getMembers();
        this.memberReplicationContexts = new MemberReplicationContext[clusterMembers.length];
        for (int i = 0; i < clusterMembers.length; i++) {
            ClusterMember clusterMember = clusterMembers[i];
            if (clusterMember != this.member) {
                this.memberReplicationContexts[i] = new MemberReplicationContext(this.member.getId(), clusterMember, agent, connectionManager, state);
            }
        }
        // TODO: also handle heartbeats here
    }

    /**
     * Update cluster, potentially adding/removing servers for replication.
     *
     * @param cluster the updated cluster
     */
    public void onClusterChange(Cluster cluster) {
        ClusterMember[] clusterMembers = cluster.getMembers();
        MemberReplicationContext[] newReplicationContexts = new MemberReplicationContext[clusterMembers.length];
        boolean serversAdded = false;
        for (int i = 0; i < clusterMembers.length; i++) {
            ClusterMember clusterMember = clusterMembers[i];
            buildContext:
            if (clusterMember != this.member) {
                for (int j = 0; j < this.memberReplicationContexts.length; j++) {
                    MemberReplicationContext ctx = this.memberReplicationContexts[i];
                    if (ctx != null && ctx.getTargetMember() == clusterMember) {
                        newReplicationContexts[i] = ctx;
                        break buildContext;
                    }
                }
                serversAdded = true;
                newReplicationContexts[i] = new MemberReplicationContext(this.member.getId(), clusterMember, this.agent, this.connectionManager, this.state);
            }
        }
        this.memberReplicationContexts = newReplicationContexts;
        if (serversAdded) {
            // New round of replication with servers that are not seen as up-to-date
            replicate();
        }
    }

    /**
     * Ask to replicate latest entry
     *
     * @return once the entry is "committed" (enough servers have agreed)
     */
    public CompletionStage<Void> replicate() {
        return new EntryReplicationContext(this.agent, this.state.getLog(), this.memberReplicationContexts).getFuture();
    }
}
