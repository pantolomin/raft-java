package net.pantolomin.raft.api;

import net.pantolomin.raft.domain.AppendEntries;
import net.pantolomin.raft.domain.ClusterMember;
import net.pantolomin.raft.domain.RequestVote;

import java.util.concurrent.CompletionStage;

public interface RequestHandler {
    CompletionStage<AppendEntries.Response> handle(ClusterMember member, AppendEntries appendEntries);

    CompletionStage<RequestVote.Response> handle(ClusterMember member, RequestVote request);
}
