/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

import io.kroxylicious.proxy.config.BaseConfig;

/**
 * Defines the details of how Contributions interact with configuration for {@link BaseContributor}'s purposes.
 *
 * @param configurationType defines the expected class for configuration objects
 */
public record ConfigurationDefinition(Class<? extends BaseConfig> configurationType) {}