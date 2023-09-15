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
 * @param configurationRequired {@code true} if the contribution requires configuration. {@code false} if the contribution uses configuration for optional properties or to override defaults.
 */
public record ConfigurationDefinition(Class<? extends BaseConfig> configurationType, boolean configurationRequired) {}