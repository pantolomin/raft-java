package net.pantolomin.raft.domain;

public record LogEntry(int term, Object command) {
    // nothing else needed
}
