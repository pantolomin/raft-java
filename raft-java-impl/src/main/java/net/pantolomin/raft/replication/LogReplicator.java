package net.pantolomin.raft.replication;

import lombok.RequiredArgsConstructor;
import net.pantolomin.raft.Agent;
import net.pantolomin.raft.Cluster;
import net.pantolomin.raft.Log;
import net.pantolomin.raft.api.ClusterMember;
import net.pantolomin.raft.api.ConnectionManager;
import net.pantolomin.raft.domain.State;

@RequiredArgsConstructor
public class LogReplicator {
    private final ClusterMember member;
    private final Agent agent;
    private final ConnectionManager connectionManager;
    private final State state;
    private Cluster cluster;
    private MemberReplicationContext[] memberReplicationContexts;

    public LogReplicator(Cluster cluster, ClusterMember member, Agent agent, ConnectionManager connectionManager, State state) {
        this.cluster = cluster;
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
    }

    public void replicate() {
        ClusterMember[] clusterMembers = cluster.getMembers();
        Log log = this.state.getLog();
        int lastIndex = log.getLastIndex();
        for (int i = 0; i < clusterMembers.length; i++) {
            ClusterMember clusterMember = clusterMembers[i];
            if (clusterMember == this.member) {
                continue;
            }
            MemberReplicationContext replicationContext = this.memberReplicationContexts[i];
            replicationContext.replicate();
        }
    }

    public void onClusterChange(Cluster cluster) {
        this.agent.run(() -> {
            this.cluster = cluster;
            // Potentially new round of replication with servers that are not seen as up-to-date
        });
    }

}
