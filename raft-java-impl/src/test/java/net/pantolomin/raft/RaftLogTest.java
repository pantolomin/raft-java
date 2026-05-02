package net.pantolomin.raft;

import lombok.extern.slf4j.Slf4j;
import net.pantolomin.raft.api.RaftLog;
import net.pantolomin.raft.domain.LogEntry;
import net.pantolomin.raft.log.RaftLogMemory;
import org.junit.Test;

import static org.junit.Assert.*;

@Slf4j
public class RaftLogTest {
    @Test
    public void testEmpty() {
        RaftLog entries = new RaftLogMemory();
        assertEquals(0, entries.getLastIndex());
        assertNull(entries.getLast());
    }

    @Test
    public void testAddAndGrow() {
        RaftLog entries = new RaftLogMemory();
        for (int i = 0; i < 800; i++) {
            int index = i + 1;
            LogEntry entry = new LogEntry(1, index);
            entries.add(entry);
            assertEquals(index, entries.getLastIndex());
            assertSame(entry, entries.getLast());
        }
    }

    @Test
    public void testGetEntries() {
        RaftLog entries = new RaftLogMemory();
        assertEquals(0, entries.getEntries(5).length);
        for (int i = 0; i < 100; i++) {
            entries.add(new LogEntry(1, i + 1));
        }
        assertEquals(100, entries.getEntries(-5).length);
        assertEquals(96, entries.getEntries(5).length);
        assertEquals(1, entries.getEntries(100).length);
        assertEquals(0, entries.getEntries(101).length);
        for (int i = 0; i < 500; i++) {
            entries.add(new LogEntry(1, i + 1));
        }
        assertEquals(500, entries.getEntries(101).length);
        assertEquals(51, entries.getEntries(550).length);
    }
}
