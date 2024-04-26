/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.multitenant;

import java.util.Objects;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.multitenant.config.MultiTenantConfig;
import io.kroxylicious.proxy.plugin.Plugin;

@Plugin(configType = MultiTenantConfig.class)
public class MultiTenantTransformationFilterFactory implements FilterFactory<MultiTenantConfig, MultiTenantConfig> {

    private static final MultiTenantConfig DEFAULT_TENANT_CONFIG = new MultiTenantConfig(null);

    @Override
    public MultiTenantConfig initialize(FilterFactoryContext context, MultiTenantConfig config) {
        return config;
    }

    @Override
    public MultiTenantTransformationFilter createFilter(FilterFactoryContext context, MultiTenantConfig configuration) {
        return new MultiTenantTransformationFilter(Objects.requireNonNullElse(configuration, DEFAULT_TENANT_CONFIG));
    }
}
