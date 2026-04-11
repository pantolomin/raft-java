package net.pantolomin.raft.api;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import net.pantolomin.raft.domain.AppendEntries;
import net.pantolomin.raft.domain.RequestVote;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class ConnectionManagerImpl implements ConnectionManager {
    private final Map<Integer, AppendEntriesContext> appendEntries = new ConcurrentHashMap<>();

    public void clear() {
        this.appendEntries.clear();
    }

    @Override
    public void subscribe(RequestHandler handler) {
        // TODO
    }

    @Override
    public void unsubscribe(RequestHandler handler) {
        // TODO
    }

    @Override
    public CompletionStage<AppendEntries.Response> send(ClusterMember member, AppendEntries appendEntries) {
        AppendEntriesContext entriesContext = new AppendEntriesContext(appendEntries);
        this.appendEntries.put(member.getId(), entriesContext);
        return entriesContext.response;
    }

    @Override
    public CompletionStage<RequestVote.Response> send(ClusterMember member, RequestVote request) {
        // TODO
        return null;
    }

    // ************************************************************************
    // ************************************************************************
    // STEPS
    // ************************************************************************
    // ************************************************************************

    public AppendEntriesContext thenMemberReceivedAppendEntries(int memberId) {
        return await().atMost(5L, TimeUnit.SECONDS).until(() -> this.appendEntries.remove(memberId), Objects::nonNull);
    }

    @RequiredArgsConstructor
    public static final class AppendEntriesContext {
        @Getter
        private final AppendEntries message;
        private final CompletableFuture<AppendEntries.Response> response = new CompletableFuture<>();

        public void success(int term) {
            this.response.complete(new AppendEntries.Response(term, true));
        }

        public void failed(int term) {
            this.response.complete(new AppendEntries.Response(term, false));
        }
    }
}
