package net.pantolomin.raft;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.concurrent.CompletionException;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Utils {
    public static void sleep(long timeMs) {
        try {
            Thread.sleep(timeMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
        }
    }
}
