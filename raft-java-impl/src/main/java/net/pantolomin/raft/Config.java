package net.pantolomin.raft;

import lombok.Builder;
import lombok.Getter;

import java.util.concurrent.TimeUnit;

@Builder
@Getter
public class Config {
    private long heartbeatInterval = 5L;
    private TimeUnit heartbeatUnit = TimeUnit.SECONDS;
    private long electionTimeout = 5L;
    private TimeUnit electionTimeoutUnit = TimeUnit.SECONDS;
}
