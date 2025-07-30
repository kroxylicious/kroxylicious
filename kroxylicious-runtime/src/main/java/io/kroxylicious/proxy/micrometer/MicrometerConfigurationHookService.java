/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.micrometer;

/**
 * Service to build a MicrometerConfigurationHook for a configuration. Implementations should be
 * annotated with {@link io.kroxylicious.proxy.plugin.Plugin} and added to the ServiceLoader
 * metadata for this interface.
 * @param <T> config type
 */
public interface MicrometerConfigurationHookService<T> {

    /**
     * Build a hook
     * @param config config
     * @return hook
     */
    MicrometerConfigurationHook build(T config);

}
