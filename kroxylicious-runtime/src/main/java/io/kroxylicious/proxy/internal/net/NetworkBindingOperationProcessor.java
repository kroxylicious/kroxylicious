/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import io.netty.bootstrap.ServerBootstrap;

/**
 * Processes {@link NetworkBindingOperation}s on a suitable {@link ServerBootstrap}.  Once the
 * operation is complete, the processor will complete the {@link NetworkBindingOperation#getFuture()}.
 */
public interface NetworkBindingOperationProcessor extends AutoCloseable {

    /**
     * Starts the processor running.
     *
     * @param plain plain bootstrap
     * @param tls tls bootstrap
     */
    void start(ServerBootstrap plain, ServerBootstrap tls);

    /**
     * Enqueues a network binding operation. Once the operation is complete the processor
     * will complete the {@link NetworkBindingOperation#getFuture()}.
     *
     * @param o operation to be performed.
     */
    void enqueueNetworkBindingEvent(NetworkBindingOperation<?> o);

    /**
     * Closes the processor.
     */
    void close();
}
