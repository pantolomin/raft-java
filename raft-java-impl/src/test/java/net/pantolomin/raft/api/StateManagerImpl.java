package net.pantolomin.raft.api;

import lombok.RequiredArgsConstructor;
import net.pantolomin.raft.domain.LogEntry;

import java.util.List;

@RequiredArgsConstructor
public final class StateManagerImpl implements StateManager {
    private final List<LogEntry> entries;

    public StateManagerImpl() {
        this(List.of());
    }

    @Override
    public List<LogEntry> loadState() {
        return List.of();
    }
}
