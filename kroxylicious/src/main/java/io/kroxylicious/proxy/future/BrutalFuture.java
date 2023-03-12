package io.kroxylicious.proxy.future;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class BrutalFuture<T> extends CompletableFuture<T> {

    public boolean internalComplete(T value) {
        return super.complete(value);
    }

    public boolean internalCompleteExceptionally(Throwable ex) {
        return super.completeExceptionally(ex);
    }

    @Override
    public boolean complete(T value) {
        throw new RuntimeException("don't complete me");
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        throw new RuntimeException("don't complete me");
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        throw new RuntimeException("don't block me");
    }

}
