/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.config.BaseConfig;

/**
 * FilterContributor is a pluggable source of Kroxylicious filter implementations.
 */
public interface FilterContributor {

    String getTypeName();

    default Class<? extends BaseConfig> getConfigClass() {
        return BaseConfig.class;
    }

    /**
     * Creates an instance of the service.
     *
     * @param context   context containing service configuration which may be null if the service instance does not accept configuration.
     * @return the service instance, or null if this contributor does not offer this short name.
     */
    Filter getInstance(BaseConfig config, FilterConstructContext context);

}
