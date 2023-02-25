/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config.micrometer;

import io.micrometer.core.instrument.MeterRegistry;

public interface MicrometerConfigurationHook {
    void configure(MeterRegistry targetRegistry);

}
