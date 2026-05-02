package net.pantolomin.raft.domain;

import java.io.Serializable;

public record LogEntry(int term, Object command) implements Serializable {
    // nothing else needed
}
