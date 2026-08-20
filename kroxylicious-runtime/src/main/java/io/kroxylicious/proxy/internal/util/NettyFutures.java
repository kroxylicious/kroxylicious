/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.util;

import org.slf4j.Logger;

import io.netty.channel.ChannelFutureListener;

/**
 * Utility methods for working with Netty futures in terminal error paths, ensuring that
 * failures are logged rather than silently discarded.
 */
public final class NettyFutures {

    private NettyFutures() {
    }

    /**
     * Returns a {@link ChannelFutureListener} that logs a warning when the future completes
     * unsuccessfully. Intended for terminal error paths where there is no recovery to perform
     * and no caller to propagate the result into, so that exceptions are not silently dropped.
     *
     * @param log the logger to use
     * @param operation a short description of the operation, recorded as a structured log key-value
     * @return a listener that logs on failure
     */
    public static ChannelFutureListener logFailure(Logger log, String operation) {
        return future -> {
            if (!future.isSuccess() && future.cause() != null) {
                log.atWarn()
                        .setCause(future.cause())
                        .addKeyValue("operation", operation)
                        .log("Netty channel operation failed");
            }
        };
    }
}
