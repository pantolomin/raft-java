package net.pantolomin.raft.api;

import net.pantolomin.raft.domain.AppendEntries;
import net.pantolomin.raft.domain.RequestVote;

import java.util.concurrent.CompletionStage;

public interface ConnectionManager {
    void subscribe(RequestHandler handler);

    void unsubscribe(RequestHandler handler);

    CompletionStage<AppendEntries.Response> send(ClusterMember member, AppendEntries appendEntries);

    CompletionStage<RequestVote.Response> send(ClusterMember member, RequestVote request);
}
