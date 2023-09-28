/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.multitenant;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterCreationContext;

import static org.assertj.core.api.Assertions.assertThat;

class MultiTenantFilterFactoryTest {

    @Test
    void testGetConfigTypeViaConfigurationDefinition() {
        MultiTenantTransformationFilterFactory factory = new MultiTenantTransformationFilterFactory();
        assertThat(factory.configType()).isEqualTo(Void.class);
    }

    @Test
    void testGetInstance() {
        MultiTenantTransformationFilterFactory factory = new MultiTenantTransformationFilterFactory();
        Filter filter = factory.createFilter(Mockito.mock(FilterCreationContext.class), null);
        assertThat(filter).isNotNull().isInstanceOf(MultiTenantTransformationFilter.class);
    }

}