/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.multitenant;

import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;

@Plugin(configType = Void.class)
public class MultiTenantTransformationFilterFactory implements FilterFactory<Void, Void> {

    @Override
    public Void initialize(FilterFactoryContext context, Void config) {
        return null;
    }

    @Override
    public MultiTenantTransformationFilter createFilter(FilterFactoryContext context, Void configuration) {
        return new MultiTenantTransformationFilter();
    }
}
