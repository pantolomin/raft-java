package net.pantolomin.raft;

import lombok.Getter;
import net.pantolomin.raft.api.ClusterMember;

@Getter
public class Cluster {
    private ClusterMember[] members;
}
