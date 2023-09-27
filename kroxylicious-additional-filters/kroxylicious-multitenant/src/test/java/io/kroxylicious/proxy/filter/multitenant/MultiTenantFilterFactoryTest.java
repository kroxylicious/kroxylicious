/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.multitenant;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterConstructContext;

import static org.assertj.core.api.Assertions.assertThat;

class MultiTenantFilterFactoryTest {

    @Test
    void testGetConfigTypeViaConfigurationDefinition() {
        MultiTenantTransformationFilter.Factory factory = new MultiTenantTransformationFilter.Factory();
        assertThat(factory.getConfigType()).isEqualTo(Void.class);
    }

    @Test
    void testGetInstance() {
        MultiTenantTransformationFilter.Factory factory = new MultiTenantTransformationFilter.Factory();
        Filter filter = factory.createInstance(Mockito.mock(FilterConstructContext.class));
        assertThat(filter).isNotNull().isInstanceOf(MultiTenantTransformationFilter.class);
    }

}