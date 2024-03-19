/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.audit;

import edu.umd.cs.findbugs.annotations.NonNull;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

public interface EventSinkService<C> {

    /**
     * Validates the configuration.
     * @param config configuration
     * @throws PluginConfigurationException when the configuration is invalid
     */
    void validateConfiguration(C config) throws PluginConfigurationException;

    @NonNull
    default C requireConfig(C config) {
        if (config == null) {
            throw new PluginConfigurationException(this.getClass().getSimpleName() + " requires configuration, but config object is null");
        }
        return config;
    }

    EventSink createAuditSink(C configuration);

}
