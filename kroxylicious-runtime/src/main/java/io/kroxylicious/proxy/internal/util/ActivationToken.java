/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A thread-safe state that ensures exactly-once increment and decrement operations.
 * <p>
 * This counter prevents race conditions in connection lifecycle management by tracking
 * its own state and only allowing state transitions: INITIAL → ACTIVE → INACTIVE.
 * Multiple calls to increment/decrement methods are safe and idempotent.
 * </p>
 *
 * <p><strong>Thread Safety:</strong> This class is thread-safe and can be used
 * concurrently from multiple threads.</p>
 */
public class ActivationToken {

    // state values
    private static final int INITIAL_VALUE = 0;
    private static final int ACQUIRED = 1;
    private static final int RELEASED = -1;

    // the state of this instance
    private final AtomicInteger state = new AtomicInteger(INITIAL_VALUE);

    // the tracker of all active instances
    private final AtomicInteger currentCounter;

    public ActivationToken(AtomicInteger currentCounter) {
        this.currentCounter = currentCounter;
    }

    /**
     * Attempts to increment the counter if it is in the INITIAL state.
     * If the state is already ACQUIRED or RELEASED, this method has no effect.
     */
    public void acquire() {
        if (this.state.compareAndSet(INITIAL_VALUE, ACQUIRED)) {
            this.currentCounter.incrementAndGet();
        }
    }

    public void release() {
        if (this.state.compareAndSet(ACQUIRED, RELEASED)) {
            this.currentCounter.decrementAndGet();
        }
    }
}
