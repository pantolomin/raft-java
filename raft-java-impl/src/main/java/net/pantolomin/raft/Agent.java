package net.pantolomin.raft;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import java.util.function.Supplier;

@Slf4j
public class Agent {
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    public CompletionStage<Void> run(Runnable task) {
        return CompletableFuture.runAsync(task, this.executorService);
    }

    public <T> CompletionStage<T> ask(Supplier<T> task) {
        return CompletableFuture.supplyAsync(task, this.executorService);
    }

    public ScheduledFuture<CompletionStage<Void>> schedule(Runnable task, long delay, TimeUnit unit) {
        return this.scheduledExecutorService.schedule(() -> run(task), delay, unit);
    }

    public ScheduledFuture<?> schedule(Runnable task, long delay, long interval, TimeUnit unit) {
        return this.scheduledExecutorService.scheduleAtFixedRate(() -> run(task), delay, interval, unit);
    }

    /**
     * Stop the agent
     */
    public void stop() {
        log.info("Stopping RAFT agent");
        this.scheduledExecutorService.shutdownNow();
        try {
            if (!this.scheduledExecutorService.awaitTermination(10L, TimeUnit.SECONDS)) {
                log.warn("Failed to end all RAFT scheduled tasks - timeout");
            }
        } catch (InterruptedException e) {
            // Simply put the flag back
            Thread.currentThread().interrupt();
            log.warn("Failed to end all RAFT scheduled tasks - interrupted");
        }
        this.executorService.shutdownNow();
    }
}
