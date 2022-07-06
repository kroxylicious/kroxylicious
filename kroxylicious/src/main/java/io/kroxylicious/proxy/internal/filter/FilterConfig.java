/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Base class for all filter-specific configuration types.
 * Subclasses should be immutable and have a constructor annotated with {@link JsonCreator}.
 */
public class FilterConfig {
}
