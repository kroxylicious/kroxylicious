/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;

public class DefaultNetworkBindingOperationProcessor implements NetworkBindingOperationProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultNetworkBindingOperationProcessor.class);

    /** Queue of network binding operations */
    private final BlockingQueue<NetworkBindingOperation<?>> queue = new LinkedBlockingQueue<>();

    private final AtomicBoolean running = new AtomicBoolean();

    /** Executor that performs the binding operations */
    private final ExecutorService networkBindingExecutor = Executors.newFixedThreadPool(2, r -> {
        var t = new Thread(r);
        t.setDaemon(true);
        return t;
    });

    private static final NetworkBindingOperation<Void> POISON_PILL = new NetworkBindingOperation<>(false) {
        CompletableFuture<Void> swallowed = new CompletableFuture<>();

        @Override
        public int port() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Void> getFuture() {
            return swallowed;
        }

        @Override
        public void performBindingOperation(ServerBootstrap serverBootstrap, ExecutorService executorService) {
            throw new UnsupportedOperationException();
        }
    };

    @Override
    public void enqueueNetworkBindingEvent(NetworkBindingOperation<?> o) {
        queue.add(o);
    }

    @Override
    public void start(ServerBootstrap plainServerBootstrap, ServerBootstrap tlsServerBootstrap) {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        networkBindingExecutor.submit(() -> {
            try {
                do {
                    var networkBindingOperation = queue.take();
                    if (networkBindingOperation == POISON_PILL) {
                        return;
                    }

                    var bootstrap = networkBindingOperation.tls() ? tlsServerBootstrap : plainServerBootstrap;
                    try {
                        networkBindingOperation.performBindingOperation(bootstrap, networkBindingExecutor);
                    }
                    catch (Exception e) {
                        // We don't expect performBindingOperation to throw an exception but if it does,
                        // we don't want to break the executor loop.
                        LOGGER.error("Unexpected error performing the binding operation", e);
                    }
                } while (!Thread.interrupted());
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            finally {
                POISON_PILL.getFuture().complete(null);
                networkBindingExecutor.shutdown();
                LOGGER.debug("Network event processor shutdown");
            }
        });
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            networkBindingExecutor.shutdown();
            return;
        }

        enqueueNetworkBindingEvent(POISON_PILL);
        boolean shutdown = false;
        try {
            POISON_PILL.getFuture().get(1, TimeUnit.MINUTES);
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException(e);
        }
        finally {
            if (!shutdown) {
                networkBindingExecutor.shutdownNow();
            }
        }
    }
}
