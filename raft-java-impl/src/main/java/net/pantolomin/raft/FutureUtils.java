package net.pantolomin.raft;

import java.util.concurrent.*;

public class FutureUtils {
    public static Throwable unwrap(Throwable throwable) {
        if (throwable instanceof CompletionException c) {
            return c.getCause();
        }
        return throwable;
    }

    public static <T> T get(CompletionStage<T> future) {
        try {
            return future.toCompletableFuture().get(5L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
        } catch (ExecutionException | TimeoutException e) {
            throw new CompletionException(e);
        }
    }
}
