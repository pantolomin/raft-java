package net.pantolomin.raft.api;

import net.pantolomin.raft.domain.LogEntry;

import java.util.List;

public interface StateManager {
    List<LogEntry> loadState();
}
