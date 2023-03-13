package io.kroxylicious.proxy.future;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalFuture<T> extends CompletableFuture<T> {

    private static final Logger logger = LoggerFactory.getLogger(InternalFuture.class);

    public boolean internalComplete(T value) {
        return super.complete(value);
    }

    public boolean internalCompleteExceptionally(Throwable ex) {
        return super.completeExceptionally(ex);
    }

    @Override
    public boolean complete(T value) {
        logger.debug("Internal future was completed unexpectedly with value {}!", value);
        throw new UnsupportedOperationException("Internal future completed unexpectedly");
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        logger.debug("Internal future was completed unexpectedly with throwable!", ex);
        throw new UnsupportedOperationException("Internal future completed unexpectedly");
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        logger.debug("Blocking get() called on InternalFuture, we don't want to block the proxy event loop");
        throw new UnsupportedOperationException("Blocking operations unsupported");
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        logger.debug("Blocking get(timeout, unit) called on InternalFuture, we don't want to block the proxy event loop");
        throw new UnsupportedOperationException("Blocking operations unsupported");
    }
}
