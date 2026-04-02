package net.pantolomin.raft.domain;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class LogEntry {
    private final int term;
    private final Object command;
}
