package net.pantolomin.raft;

import lombok.Builder;
import lombok.Getter;

import java.util.concurrent.TimeUnit;

@Builder
@Getter
public class Config {
    private final long heartbeatInterval = 5L;
    private final TimeUnit heartbeatUnit = TimeUnit.SECONDS;
    private final long electionTimeout = 5L;
    private final TimeUnit electionTimeoutUnit = TimeUnit.SECONDS;
}
