/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

/**
 * Base class for all filter-specific configuration types.
 * Subclasses should be immutable. Deserialized by Jackson currently so annotate with jackson annotations if
 * required to specialise construction.
 */
public class FilterConfig {
}
