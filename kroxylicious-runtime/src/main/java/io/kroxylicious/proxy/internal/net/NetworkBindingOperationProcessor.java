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
     * @param serverBootstrap bootstrap on which binding operations are performed
     */
    void start(ServerBootstrap serverBootstrap);

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
    @Override
    void close();
}
