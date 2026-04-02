package net.pantolomin.raft;

import lombok.Getter;
import net.pantolomin.raft.api.ClusterMember;

import java.util.List;

@Getter
public class Cluster {
    private List<ClusterMember> members;
}
