package net.pantolomin.raft.domain;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import net.pantolomin.raft.api.RaftLog;

@AllArgsConstructor
@Getter
public class State {
    // ************************************************************************
    // Persistent state on all servers:
    // (Updated on stable storage before responding to RPCs)
    // ************************************************************************

    /**
     * Log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
     */
    private final RaftLog raftLog;

    /**
     * Latest term server has seen (initialized to 0 on first boot, increases monotonically)
     */
    @Setter
    private int currentTerm;
}
