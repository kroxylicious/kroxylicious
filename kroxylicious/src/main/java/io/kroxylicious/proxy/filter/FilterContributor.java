/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.filter;

import io.kroxylicious.proxy.internal.filter.FilterConfig;

public interface FilterContributor {

    Class<? extends FilterConfig> getConfigType(String shortName);

    KrpcFilter getFilter(String shortName, FilterConfig config);
}
