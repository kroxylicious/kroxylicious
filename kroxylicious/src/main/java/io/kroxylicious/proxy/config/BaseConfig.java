/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * <p>
 * Base class for all configuration types. Users ar able to create arbitrary jackson
 * deserializable config classes that can be integrated With the Kroxylicious configuration file.
 * </p>
 * <p>
 * Subclasses should be immutable and have a constructor annotated with {@link JsonCreator}.
 * </p>
 */
public class BaseConfig {
}
