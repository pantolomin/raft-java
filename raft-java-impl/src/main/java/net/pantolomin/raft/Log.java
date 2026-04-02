package net.pantolomin.raft;

import lombok.extern.slf4j.Slf4j;
import net.pantolomin.raft.domain.LogEntry;

@Slf4j
public class Log {
    private LogBehavior logBehavior;
    private LogEntry[] entries;
    private int entriesMask;
    private int baseIndex;
    private int head;
    private int tail;
    private int commitIndex;

    public Log() {
        this.logBehavior = new EmptyLogBehavior();
        this.entries = new LogEntry[64];
        this.entriesMask = this.entries.length - 1;
        this.head = 0;
        this.tail = -1;
        this.baseIndex = 1;
        this.commitIndex = 0;
    }

    public int getFirstIndex() {
        return this.logBehavior.getFirstIndex();
    }

    public int getLastIndex() {
        return this.logBehavior.getLastIndex();
    }

    public LogEntry getLast() {
        return this.logBehavior.getLast();
    }

    public void add(LogEntry entry) {
        this.logBehavior.add(entry);
    }

    public void commit(int index) {
        this.commitIndex = index;
    }

    private void growEntries() {
        int newSize = this.entries.length * 2;
        log.warn("Too many uncommited entries, need to make log bigger: {}", newSize);
        LogEntry[] newEntries = new LogEntry[newSize];
        int newHead = this.entries.length + this.head;
        System.arraycopy(this.entries, this.head, newEntries, newHead, this.entries.length - this.head);
        System.arraycopy(this.entries, 0, newEntries, 0, this.tail);
        this.entries = newEntries;
        this.entriesMask = newSize - 1;
        this.head = newHead;
    }

    private interface LogBehavior {
        int getFirstIndex();

        int getLastIndex();

        LogEntry getLast();

        void add(LogEntry entry);
    }

    private final class EmptyLogBehavior implements LogBehavior {
        @Override
        public int getFirstIndex() {
            return 0;
        }

        @Override
        public int getLastIndex() {
            return 0;
        }

        @Override
        public LogEntry getLast() {
            return null;
        }

        @Override
        public void add(LogEntry entry) {
            tail = 0;
            entries[tail] = entry;
            logBehavior = new NormalLogBehavior();
        }
    }

    private class NormalLogBehavior implements LogBehavior {
        @Override
        public int getFirstIndex() {
            return baseIndex;
        }

        @Override
        public int getLastIndex() {
            int size = tail - head;
            return baseIndex + (size >= 0 ? size : entries.length + size);
        }

        @Override
        public LogEntry getLast() {
            return entries[tail];
        }

        @Override
        public void add(LogEntry entry) {
            tail = (tail + 1) & entriesMask;
            if (tail == head) {
                if (commitIndex <= baseIndex) {
                    // Need to grow the entries
                    growEntries();
                } else {
                    // Remove oldest element from the log
                    head = (head + 1) & entriesMask;
                    baseIndex++;
                    logBehavior = new FullLogBehavior();
                }
            }
            entries[tail] = entry;
        }
    }

    private final class FullLogBehavior extends NormalLogBehavior {
        @Override
        public int getLastIndex() {
            return baseIndex + entriesMask; // entries.length - 1
        }

        @Override
        public void add(LogEntry entry) {
            tail = (tail + 1) & entriesMask;
            if (commitIndex <= baseIndex) {
                // Need to grow the entries
                growEntries();
                logBehavior = new NormalLogBehavior();
            } else {
                // Remove oldest element from the log
                head = (head + 1) & entriesMask;
                baseIndex++;
            }
            entries[tail] = entry;
        }
    }
}
