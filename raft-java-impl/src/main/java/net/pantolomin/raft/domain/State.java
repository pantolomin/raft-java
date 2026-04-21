package net.pantolomin.raft.domain;

import lombok.Getter;
import lombok.Setter;
import net.pantolomin.raft.Log;

@Getter
public class State {
    // ************************************************************************
    // Persistent state on all servers:
    // (Updated on stable storage before responding to RPCs)
    // ************************************************************************

    /**
     * Log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
     */
    private final Log log = new Log();

    /**
     * Latest term server has seen (initialized to 0 on first boot, increases monotonically)
     */
    @Setter
    private int currentTerm;
}
