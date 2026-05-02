package net.pantolomin.raft.api;

import net.pantolomin.raft.domain.AppendEntries;
import net.pantolomin.raft.domain.ClusterMember;
import net.pantolomin.raft.domain.RequestVote;

import java.util.concurrent.CompletionStage;

public interface ConnectionManager {
    void start();

    void stop();

    void subscribe(RequestHandler handler);

    void unsubscribe(RequestHandler handler);

    CompletionStage<AppendEntries.Response> send(ClusterMember member, AppendEntries appendEntries);

    CompletionStage<RequestVote.Response> send(ClusterMember member, RequestVote request);
}
