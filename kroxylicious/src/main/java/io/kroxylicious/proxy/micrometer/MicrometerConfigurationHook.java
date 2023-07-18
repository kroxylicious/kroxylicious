/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.micrometer;

import io.micrometer.core.instrument.MeterRegistry;

public interface MicrometerConfigurationHook extends AutoCloseable {
    /**
     * Configures this hook into the given registry.
     *
     * @param targetRegistry meter registry.
     */
    void configure(MeterRegistry targetRegistry);

    /**
     * Releases resources that may have been registered by this hook.  Implementations
     * of this method must not throw unchecked exceptions.
     */
    default void close() {
    }

}
