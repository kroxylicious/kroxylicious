/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.util.concurrent.Future;

/**
 * Abstract encapsulation of a network binding operation.
 * @param <U> the type yielded by the future signalling the completion of the binding operation.
 */
public abstract class NetworkBindingOperation<U> {

    protected final boolean tls;

    /**
     * Creates a NetworkBindingOperation with given TLS mode.
     * @param tls Indicates whether the port is to be used for TLS.
     */
    protected NetworkBindingOperation(boolean tls) {
        this.tls = tls;
    }

    /**
     * Indicates whether the binding is to be used for TLS.
     *
     * @return tls flag
     */
    public boolean tls() {
        return tls;
    }

    /**
     * The port associated with the binding operation.
     *
     * @return port
     */
    public abstract int port();

    /**
     * The future that signals the completion of the network binding operation.
     * @return completable future
     */
    public abstract CompletableFuture<U> getFuture();

    /**
     * Performs the binding operation.   Implementations must organise of the future provided by {@link #getFuture()}
     * to be {@link CompletableFuture#complete(Object)} when the binding operation completes successfully.  If the
     * binding operations fails, the future must be {@link CompletableFuture#completeExceptionally(Throwable)}.
     * <br/>
     * If the implementation makes use of {@link io.netty.util.concurrent.FutureListener}, the implementation of the
     * {@link io.netty.util.concurrent.FutureListener#operationComplete(Future)} method must delegate work to the
     * executorService rather than occupy the Netty owned worker thread.
     *
     * @param serverBootstrap server bootstrap on which the binding operations must be performed.
     * @param executorService executor service
     */
    public abstract void performBindingOperation(ServerBootstrap serverBootstrap, ExecutorService executorService);
}
