package net.pantolomin.raft;

import lombok.extern.slf4j.Slf4j;
import net.pantolomin.raft.domain.LogEntry;
import org.junit.Test;

import static org.junit.Assert.*;

@Slf4j
public class LogRingTest {
    @Test
    public void testEmpty() {
        LogRing entries = new LogRing();
        assertEquals(0, entries.getFirstIndex());
        assertEquals(0, entries.getLastIndex());
        assertNull(entries.getLast());
    }

    @Test
    public void testAddAndGrow() {
        LogRing entries = new LogRing();
        for (int i = 0; i < 100; i++) {
            int index = i + 1;
            LogEntry entry = new LogEntry(1, index);
            entries.add(entry);
            assertEquals(1, entries.getFirstIndex());
            assertEquals(index, entries.getLastIndex());
            assertSame(entry, entries.getLast());
        }
    }

    @Test
    public void testAddAndGrowEnd() {
        LogRing entries = new LogRing();
        int i = 0;
        for (; i < 64; i++) {
            int index = i + 1;
            LogEntry entry = new LogEntry(1, index);
            entries.add(entry);
            assertEquals(1, entries.getFirstIndex());
            assertEquals(index, entries.getLastIndex());
            assertSame(entry, entries.getLast());
        }
        entries.commit(64);
        for (; i < 130; i++) {
            int index = i + 1;
            LogEntry entry = new LogEntry(1, index);
            entries.add(entry);
            assertEquals(Math.min(index - 63, 64), entries.getFirstIndex());
            assertEquals(index, entries.getLastIndex());
            assertSame(entry, entries.getLast());
        }
    }
}
