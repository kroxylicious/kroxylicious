/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.multitenant;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.filter.multitenant.config.MultiTenantConfig;

import static org.assertj.core.api.Assertions.assertThat;

class MultiTenantTest {

    @Test
    void createFilter() {
        var factory = new MultiTenant();
        Filter filter = factory.createFilter(Mockito.mock(FilterFactoryContext.class), Mockito.mock(MultiTenantConfig.class));
        assertThat(filter).isNotNull().isInstanceOf(MultiTenantFilter.class);
    }

}
