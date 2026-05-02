package net.pantolomin.raft.api;

import net.pantolomin.raft.domain.LogEntry;

public interface RaftLog {
    int getLastIndex();

    int getLastTerm();

    int getCommitIndex();

    LogEntry getLast();

    void add(int prevIndex, LogEntry[] entries);

    void add(LogEntry entry);

    void commit(int index);

    LogEntry getEntry(int index);

    LogEntry[] getEntries(int fromIndex);
}
