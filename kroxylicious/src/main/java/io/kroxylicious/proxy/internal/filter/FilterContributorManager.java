/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.filter;

import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterConstructContext;
import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.FilterExecutors;

public class FilterContributorManager {

    private static final FilterContributorManager INSTANCE = new FilterContributorManager();

    private final ServiceLoader<FilterContributor> contributors;

    private FilterContributorManager() {
        this.contributors = ServiceLoader.load(FilterContributor.class);
    }

    public static FilterContributorManager getInstance() {
        return INSTANCE;
    }

    public Class<? extends BaseConfig> getConfigType(String shortName) {
        Iterator<FilterContributor> it = contributors.iterator();
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
        Iterator<FilterContributor> it = contributors.iterator();
        while (it.hasNext()) {
            FilterContributor contributor = it.next();
            Filter filter = contributor.getInstance(shortName, new FilterConstructContext() {
                @Override
                public FilterExecutors executors() {
                    return new FilterExecutors() {
                        @Override
                        public ScheduledExecutorService eventLoop() {
                            return Executors.newScheduledThreadPool(1);
                        }
                    };
                }

                @Override
                public BaseConfig getConfig() {
                    return filterConfig;
                }
            });
            if (filter != null) {
                return filter;
            }
        }

        throw new IllegalArgumentException("No filter found for name '" + shortName + "'");
    }
}
