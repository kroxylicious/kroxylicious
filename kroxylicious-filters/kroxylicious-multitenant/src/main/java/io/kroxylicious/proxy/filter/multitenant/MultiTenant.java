/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.multitenant;

import java.util.Objects;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.multitenant.config.MultiTenantConfig;
import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link FilterFactory} for {@link MultiTenantFilter}.
 */
@Plugin(configType = MultiTenantConfig.class)
public class MultiTenant implements FilterFactory<MultiTenantConfig, MultiTenantConfig> {

    private static final MultiTenantConfig DEFAULT_TENANT_CONFIG = new MultiTenantConfig(null);

    @Override
    public @Nullable MultiTenantConfig initialize(FilterFactoryContext context, @Nullable MultiTenantConfig config) {
        return config;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, @Nullable MultiTenantConfig configuration) {
        return new MultiTenantFilter(Objects.requireNonNullElse(configuration, DEFAULT_TENANT_CONFIG));
    }
}
