package net.pantolomin.raft.domain;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class Cluster {
    private final ClusterMember[] members;
}
