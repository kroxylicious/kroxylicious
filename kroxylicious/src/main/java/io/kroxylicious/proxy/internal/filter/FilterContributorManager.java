/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.util.Iterator;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.function.Supplier;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterContributor;

public class FilterContributorManager {

    public static final FilterContributorManager INSTANCE = new FilterContributorManager(new Supplier<>() {

        private final ServiceLoader<FilterContributor> serviceLoader = ServiceLoader.load(FilterContributor.class);

        @Override
        public Iterator<FilterContributor> get() {
            return serviceLoader.iterator();
        }
    });

    private final Supplier<Iterator<FilterContributor>> contributors;

    /* test */
    public FilterContributorManager(Supplier<Iterator<FilterContributor>> iteratorSupplier) {
        this.contributors = iteratorSupplier;
    }

    public static FilterContributorManager getInstance() {
        return INSTANCE;
    }

    public Class<? extends BaseConfig> getConfigType(String shortName) {
        Iterator<FilterContributor> it = contributors.get();
        while (it.hasNext()) {
            FilterContributor contributor = it.next();
            Class<? extends BaseConfig> configType = contributor.getConfigType(shortName);
            if (configType != null) {
                return configType;
            }
        }

        throw new IllegalArgumentException("No filter found for name '" + shortName + "'");
    }

    public Filter getFilter(String shortName, BaseConfig filterConfig) {
        Iterator<FilterContributor> it = contributors.get();
        while (it.hasNext()) {
            FilterContributor contributor = it.next();
            Filter filter = contributor.getInstance(shortName, filterConfig);
            if (filter != null) {
                return filter;
            }
        }

        throw new IllegalArgumentException("No filter found for name '" + shortName + "'");
    }

    public boolean requiresConfig(String shortName) {
        Iterator<FilterContributor> it = contributors.get();
        while (it.hasNext()) {
            FilterContributor contributor = it.next();
            final Boolean requiresConfig = contributor.requiresConfig(shortName);
            if (!Objects.isNull(requiresConfig)) {
                return requiresConfig;
            }
        }
        return false;
    }
}
