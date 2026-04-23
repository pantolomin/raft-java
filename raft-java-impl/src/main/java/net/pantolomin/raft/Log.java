package net.pantolomin.raft;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.pantolomin.raft.domain.LogEntry;

import java.util.Arrays;

@Slf4j
public class Log {
    private static final int LOG_2_ARRAY_SIZE = 8;
    private static final int ARRAY_SIZE = 1 << LOG_2_ARRAY_SIZE;
    private static final int ARRAY_MASK = ARRAY_SIZE - 1;

    private LogEntry[][] entries;
    @Getter
    private int lastIndex;
    @Getter
    private int commitIndex;

    public Log() {
        this.entries = new LogEntry[1][ARRAY_SIZE];
        this.commitIndex = 0;
    }

    public int getLastTerm() {
        LogEntry last = getLast();
        return last != null ? last.term() : 0;
    }

    public LogEntry getLast() {
        return getEntry(this.lastIndex);
    }

    public LogEntry getEntry(int index) {
        if (index < 1 || index > this.lastIndex) {
            return null;
        }
        index--;
        int mainArrayIdx = index >> LOG_2_ARRAY_SIZE;
        int subArrayIdx = index & ARRAY_MASK;
        return this.entries[mainArrayIdx][subArrayIdx];
    }

    public void add(int prevIndex, LogEntry[] entries) {
        this.lastIndex = prevIndex;
        for (LogEntry entry : entries) {
            add(entry);
        }
    }

    public void add(LogEntry entry) {
        int mainArrayIdx = this.lastIndex >> LOG_2_ARRAY_SIZE;
        int subArrayIdx = this.lastIndex & ARRAY_MASK;
        if (mainArrayIdx >= this.entries.length) {
            this.entries = Arrays.copyOf(this.entries, this.entries.length + 1);
            this.entries[mainArrayIdx] = new LogEntry[ARRAY_SIZE];
        }
        this.entries[mainArrayIdx][subArrayIdx] = entry;
        this.lastIndex++;
    }

    public void commit(int index) {
        this.commitIndex = index;
    }

    public LogEntry[] getEntries(int fromIndex) {
        if (fromIndex > this.lastIndex) {
            return new LogEntry[0];
        }
        if (fromIndex < 1) {
            fromIndex = 1;
        }
        int remainingEntries = this.lastIndex + 1 - fromIndex;
        LogEntry[] entries = new LogEntry[remainingEntries];
        fromIndex--;
        int mainArrayIdx = fromIndex >> LOG_2_ARRAY_SIZE;
        int subArrayIdx = fromIndex & ARRAY_MASK;
        int endMainArrayIdx = (this.lastIndex - 1) >> LOG_2_ARRAY_SIZE;
        LogEntry[] currentEntries = this.entries[mainArrayIdx];
        int destPos = 0;
        while (mainArrayIdx < endMainArrayIdx) {
            int length = ARRAY_SIZE - subArrayIdx;
            System.arraycopy(currentEntries, subArrayIdx, entries, destPos, length);
            destPos += length;
            remainingEntries -= length;
            mainArrayIdx++;
            subArrayIdx = 0;
            currentEntries = this.entries[mainArrayIdx];
        }
        System.arraycopy(currentEntries, subArrayIdx, entries, destPos, remainingEntries);
        return entries;
    }
}
