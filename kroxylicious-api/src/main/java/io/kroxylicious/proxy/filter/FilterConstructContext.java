/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.service.Context;

public interface FilterConstructContext extends Context {
    FilterExecutors executors();

    static FilterConstructContext wrap(BaseConfig config, FilterExecutors executors) {
        return new FilterConstructContext() {
            @Override
            public FilterExecutors executors() {
                return executors;
            }

            @Override
            public BaseConfig getConfig() {
                return config;
            }
        };
    }
}
