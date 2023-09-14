/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.multitenant;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterConstructContext;

import static org.assertj.core.api.Assertions.assertThat;

class ContributorTest {

    @Test
    void testGetConfigType() {
        MultiTenantTransformationFilter.Contributor contributor = new MultiTenantTransformationFilter.Contributor();
        assertThat(contributor.getTypeName()).isEqualTo("MultiTenant");
    }

    @Test
    void testGetInstance() {
        MultiTenantTransformationFilter.Contributor contributor = new MultiTenantTransformationFilter.Contributor();
        Filter filter = contributor.getInstance(new BaseConfig(), Mockito.mock(FilterConstructContext.class));
        assertThat(filter).isNotNull().isInstanceOf(MultiTenantTransformationFilter.class);
    }

}
