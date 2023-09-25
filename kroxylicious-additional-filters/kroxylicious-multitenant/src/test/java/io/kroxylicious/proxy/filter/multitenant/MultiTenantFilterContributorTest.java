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

class MultiTenantFilterContributorTest {

    @Test
    void testGetConfigTypeViaConfigurationDefinition() {
        MultiTenantTransformationFilter.Contributor contributor = new MultiTenantTransformationFilter.Contributor();
        assertThat(contributor.getConfigType()).isEqualTo(Void.class);
    }

    @Test
    void testGetInstance() {
        MultiTenantTransformationFilter.Contributor contributor = new MultiTenantTransformationFilter.Contributor();
        Filter filter = contributor.getInstance(Mockito.mock(FilterConstructContext.class));
        assertThat(filter).isNotNull().isInstanceOf(MultiTenantTransformationFilter.class);
    }

}