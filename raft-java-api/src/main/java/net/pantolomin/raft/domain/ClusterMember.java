package net.pantolomin.raft.domain;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor
@Getter
@ToString
public final class ClusterMember {
    private final int id;
    private final String hostname;
    private final int port;
}
