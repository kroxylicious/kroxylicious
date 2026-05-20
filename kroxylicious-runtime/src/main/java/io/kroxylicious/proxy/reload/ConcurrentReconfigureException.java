/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.reload;

import java.io.Serial;

/**
 * Exceptional-completion cause for {@code KafkaProxy.reconfigure(Configuration)} when a
 * second reconfigure call arrives while another is in progress.
 *
 * <p>This exception is not thrown synchronously from {@code reconfigure()}; instead the
 * returned future completes exceptionally with this cause. Callers should retry, typically
 * with the most recent desired configuration.
 *
 */
public class ConcurrentReconfigureException extends RuntimeException {

    @Serial
    private static final long serialVersionUID = 1L;

    public ConcurrentReconfigureException() {
        super("a reconfigure is already in progress; the trigger should retry");
    }

    public ConcurrentReconfigureException(String message) {
        super(message);
    }
}
