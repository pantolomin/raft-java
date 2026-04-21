package net.pantolomin.raft;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.concurrent.TimeUnit;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class Config {
    private final long heartbeatInterval;
    private final TimeUnit heartbeatUnit;
    private final long electionTimeout;
    private final TimeUnit electionTimeoutUnit;

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private long heartbeatInterval = 2L;
        private TimeUnit heartbeatUnit = TimeUnit.SECONDS;
        private long electionTimeout = 10L;
        private TimeUnit electionTimeoutUnit = TimeUnit.SECONDS;

        public Builder withHeartbeatInterval(long time, TimeUnit unit) {
            this.heartbeatInterval = time;
            this.heartbeatUnit = unit;
            return this;
        }

        public Builder withElectionTimeout(long time, TimeUnit unit) {
            this.electionTimeout = time;
            this.electionTimeoutUnit = unit;
            return this;
        }

        public Config build() {
            return new Config(this.heartbeatInterval, this.heartbeatUnit, this.electionTimeout, this.electionTimeoutUnit);
        }
    }
}
