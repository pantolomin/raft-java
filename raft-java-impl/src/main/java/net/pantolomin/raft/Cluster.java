package net.pantolomin.raft;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import net.pantolomin.raft.api.ClusterMember;

@RequiredArgsConstructor
@Getter
public class Cluster {
    private final ClusterMember[] members;
}
