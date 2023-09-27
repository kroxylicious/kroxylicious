/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

public interface FilterConstructContext<B> {
    FilterExecutors executors();

    /**
     * service configuration which may be null if the service instance does not accept configuration.
     * @return config
     */
    B getConfig();
}
