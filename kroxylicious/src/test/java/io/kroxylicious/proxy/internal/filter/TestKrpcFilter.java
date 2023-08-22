/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.FilterContributorContext;
import io.kroxylicious.proxy.filter.KrpcFilter;

public record TestKrpcFilter(String shortName, BaseConfig config, FilterContributorContext context) implements KrpcFilter {
}
