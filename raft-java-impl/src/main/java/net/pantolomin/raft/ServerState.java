package net.pantolomin.raft;

public enum ServerState {
    STOPPED {
        @Override
        public boolean isStarted() {
            return false;
        }
    }, STARTING {
        @Override
        public boolean isStarted() {
            return false;
        }
    }, FOLLOWER {
        @Override
        public boolean isStarted() {
            return true;
        }
    }, CANDIDATE {
        @Override
        public boolean isStarted() {
            return true;
        }
    }, LEADER {
        @Override
        public boolean isStarted() {
            return true;
        }
    }, STOPPING {
        @Override
        public boolean isStarted() {
            return false;
        }
    };

    public abstract boolean isStarted();
}
