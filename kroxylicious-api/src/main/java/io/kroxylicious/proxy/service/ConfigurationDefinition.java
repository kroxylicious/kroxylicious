/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

/**
 * Defines the details of how Contributions interact with configuration.
 * <p>
 * configurationRequired allows filter authors to tell Kroxylicious if the filter must be supplied with a config object.
 * If the filter uses configuration to supply optional properties, or it can provide sensible default values for the configuration then it should be set to false.
 *
 * @param configurationType defines the expected class for configuration objects
 * @param configurationRequired {@code true} if the contribution requires a non-null configuration object. {@code false} if the contribution can tolerate a null configuration object
 */
public record ConfigurationDefinition(Class<?> configurationType, boolean configurationRequired) {}