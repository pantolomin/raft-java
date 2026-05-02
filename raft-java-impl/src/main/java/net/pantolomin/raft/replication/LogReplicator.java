package net.pantolomin.raft.replication;

import lombok.RequiredArgsConstructor;
import net.pantolomin.raft.Agent;
import net.pantolomin.raft.Config;
import net.pantolomin.raft.api.ConnectionManager;
import net.pantolomin.raft.domain.Cluster;
import net.pantolomin.raft.domain.ClusterMember;
import net.pantolomin.raft.domain.State;

import java.util.concurrent.CompletionStage;

@RequiredArgsConstructor
public class LogReplicator {
    private final Config config;
    private final ClusterMember member;
    private final Agent agent;
    private final ConnectionManager connectionManager;
    private final State state;
    private boolean started;
    private MemberReplicationContext[] memberReplicationContexts;

    public LogReplicator(Config config, Cluster cluster, ClusterMember member, Agent agent, ConnectionManager connectionManager, State state) {
        this.config = config;
        this.member = member;
        this.agent = agent;
        this.connectionManager = connectionManager;
        this.state = state;
        ClusterMember[] clusterMembers = cluster.getMembers();
        this.memberReplicationContexts = new MemberReplicationContext[clusterMembers.length];
        for (int i = 0; i < clusterMembers.length; i++) {
            ClusterMember clusterMember = clusterMembers[i];
            if (clusterMember != this.member) {
                this.memberReplicationContexts[i] = new MemberReplicationContext(this.member.getId(), config, clusterMember, agent, connectionManager, state);
            }
        }
    }

    public void start() {
        for (MemberReplicationContext ctx : this.memberReplicationContexts) {
            if (ctx != null) {
                ctx.start();
            }
        }
        this.started = true;
    }

    public void stop() {
        this.started = false;
        for (MemberReplicationContext ctx : this.memberReplicationContexts) {
            if (ctx != null) {
                ctx.stop();
            }
        }
    }

    /**
     * Update cluster, potentially adding/removing servers for replication.
     *
     * @param cluster the updated cluster
     */
    public void onClusterChange(Cluster cluster) {
        ClusterMember[] clusterMembers = cluster.getMembers();
        MemberReplicationContext[] newReplicationContexts = new MemberReplicationContext[clusterMembers.length];
        boolean[] remained = new boolean[this.memberReplicationContexts.length];
        boolean[] added = new boolean[newReplicationContexts.length];
        for (int i = 0; i < clusterMembers.length; i++) {
            ClusterMember clusterMember = clusterMembers[i];
            buildContext:
            if (clusterMember != this.member) {
                for (int j = 0; j < this.memberReplicationContexts.length; j++) {
                    MemberReplicationContext ctx = this.memberReplicationContexts[i];
                    if (ctx != null && ctx.getTargetMember() == clusterMember) {
                        remained[j] = true;
                        newReplicationContexts[i] = ctx;
                        break buildContext;
                    }
                }
                newReplicationContexts[i] = new MemberReplicationContext(this.member.getId(), this.config, clusterMember, this.agent, this.connectionManager, this.state);
                added[i] = true;
            }
        }
        if (this.started) {
            for (int i = 0; i < remained.length; i++) {
                if (remained[i]) {
                    continue;
                }
                MemberReplicationContext ctx = this.memberReplicationContexts[i];
                if (ctx != null) {
                    ctx.stop();
                }
            }
        }
        this.memberReplicationContexts = newReplicationContexts;
        if (this.started) {
            for (int i = 0; i < added.length; i++) {
                if (added[i]) {
                    this.memberReplicationContexts[i].start();
                }
            }
        }
    }

    /**
     * Ask to replicate latest entry
     *
     * @return once the entry is "committed" (enough servers have agreed)
     */
    public CompletionStage<Integer> replicate() {
        return new EntryReplicationContext(this.agent, this.state.getRaftLog(), this.memberReplicationContexts).getFuture();
    }
}
