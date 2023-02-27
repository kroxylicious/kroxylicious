/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.service;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.config.ProxyConfig;

/**
 * Support loading an Instance of a service, optionally providing it with configuration obtained
 * from the Kroxylicious configuration file.
 */
public interface Contributor<T> {

    Class<? extends BaseConfig> getConfigType(String shortName);

    T getInstance(String shortName, ProxyConfig proxyConfig, BaseConfig config);
}
