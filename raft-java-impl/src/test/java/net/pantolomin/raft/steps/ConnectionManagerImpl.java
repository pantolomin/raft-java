package net.pantolomin.raft.steps;

import lombok.RequiredArgsConstructor;
import net.pantolomin.raft.api.ClusterMember;
import net.pantolomin.raft.api.ConnectionManager;
import net.pantolomin.raft.api.RequestHandler;
import net.pantolomin.raft.domain.AppendEntries;
import net.pantolomin.raft.domain.RequestVote;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

@RequiredArgsConstructor
public class ConnectionManagerImpl implements ConnectionManager, RequestHandler {
    private final ClusterMember localMember;
    private final Function<ClusterMember, RequestHandler> getHandler;
    private RequestHandler handler;

    @Override
    public void subscribe(RequestHandler handler) {
        if (this.handler != null) {
            throw new IllegalStateException("Already subscribed");
        }
        this.handler = handler;
    }

    @Override
    public void unsubscribe(RequestHandler handler) {
        if (this.handler != handler) {
            throw new IllegalStateException("Subscribed for another handler");
        }
        this.handler = null;
    }

    @Override
    public CompletionStage<AppendEntries.Response> send(ClusterMember member, AppendEntries appendEntries) {
        return CompletableFuture.supplyAsync(() -> this.getHandler.apply(member).handle(this.localMember, appendEntries)).thenCompose(Function.identity());
    }

    @Override
    public CompletionStage<RequestVote.Response> send(ClusterMember member, RequestVote request) {
        return CompletableFuture.supplyAsync(() -> this.getHandler.apply(member).handle(this.localMember, request)).thenCompose(Function.identity());
    }

    @Override
    public CompletionStage<AppendEntries.Response> handle(ClusterMember member, AppendEntries appendEntries) {
        RequestHandler requestHandler = this.handler;
        if (requestHandler != null) {
            return CompletableFuture.supplyAsync(() -> requestHandler.handle(member, appendEntries)).thenCompose(Function.identity());
        }
        return new CompletableFuture<>();
    }

    @Override
    public CompletionStage<RequestVote.Response> handle(ClusterMember member, RequestVote request) {
        RequestHandler requestHandler = this.handler;
        if (requestHandler != null) {
            return CompletableFuture.supplyAsync(() -> requestHandler.handle(member, request)).thenCompose(Function.identity());
        }
        return new CompletableFuture<>();
    }
}
