package net.pantolomin.raft;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import net.pantolomin.raft.domain.ClusterMember;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class CommandResult {
    /**
     * Indicates whether the command can be applied
     */
    private final boolean success;

    /**
     * Indicates which server is the leader in the cluster (if not this one and it is known)
     */
    private final ClusterMember leader;

    static CommandResult success() {
        return new CommandResult(true, null);
    }

    static CommandResult failed() {
        return new CommandResult(false, null);
    }

    static CommandResult notLeader(ClusterMember leader) {
        return new CommandResult(false, leader);
    }
}
