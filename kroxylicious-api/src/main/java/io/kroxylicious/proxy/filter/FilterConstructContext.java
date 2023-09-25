/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.Context;

public interface FilterConstructContext<B extends BaseConfig> extends Context<B> {
    FilterExecutors executors();

    static <B extends BaseConfig> FilterConstructContext<B> wrap(B config, FilterExecutors executors) {
        return new FilterConstructContext<B>() {
            @Override
            public FilterExecutors executors() {
                return executors;
            }

            @Override
            public B getConfig() {
                return config;
            }
        };
    }
}
