/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.service.Contributor;

/**
 * FilterContributor is a pluggable source of Kroxylicious filter implementations.
 * @see Contributor
 */
public interface FilterContributor<B> extends Contributor<Filter, B, FilterConstructContext<B>> {
}
