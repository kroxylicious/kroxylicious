/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

/**
 * Context in which a Contributor is getting an instance, this includes the user-supplied configuration.
 */
public interface Context<B> {

    /**
     * service configuration which may be null if the service instance does not accept configuration.
     * @return config
     */
    B getConfig();

    static <B> Context<B> wrap(B config) {
        return () -> config;
    }

}
