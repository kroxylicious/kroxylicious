/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

/**
 * Context in which a Contributor is getting an instance, this includes the user-supplied configuration.
 * @param <B> config type
 * @deprecated we plan to remove the generic Contributor
 */
@Deprecated(since = "0.3.0")
public interface Context<B> {

    /**
     * service configuration which may be null if the service instance does not accept configuration.
     * @return config
     */
    B getConfig();

    /**
     * Wrap a config in a Context
     * @param config config
     * @return config
     * @param <B> config type
     */
    static <B> Context<B> wrap(B config) {
        return () -> config;
    }

}
