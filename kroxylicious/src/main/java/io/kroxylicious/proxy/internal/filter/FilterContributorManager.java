/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.util.Iterator;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import io.kroxylicious.proxy.config.ProxyConfig;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.KrpcFilter;

public class FilterContributorManager {

    private static final FilterContributorManager INSTANCE = new FilterContributorManager();

    private final ServiceLoader<FilterContributor> contributors;

    private FilterContributorManager() {
        this.contributors = ServiceLoader.load(FilterContributor.class);
    }

    public static FilterContributorManager getInstance() {
        return INSTANCE;
    }

    public Class<? extends FilterConfig> getConfigType(String shortName) {
        var s = Stream.concat(Stream.of(new BuiltinFilterContributor()),
                StreamSupport.stream(contributors.spliterator(), false));
        return s.map(contributor -> contributor.getConfigType(shortName))
                .filter(Objects::nonNull)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No filter found for name '" + shortName + "'"));
    }

    public KrpcFilter getFilter(String shortName, ProxyConfig proxyConfig, FilterConfig filterConfig) {
        Iterator<FilterContributor> it = contributors.iterator();
        while (it.hasNext()) {
            FilterContributor contributor = it.next();
            KrpcFilter filter = contributor.getFilter(shortName, proxyConfig, filterConfig);
            if (filter != null) {
                return filter;
            }
        }

        throw new IllegalArgumentException("No filter found for name '" + shortName + "'");
    }
}
