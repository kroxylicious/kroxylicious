/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.multitenant;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.FilterContributorContext;
import io.kroxylicious.proxy.filter.KrpcFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MultiTenantFilterContributorTest {

    private final MultiTenantFilterContributor contributor = new MultiTenantFilterContributor();

    @Test
    void testGetConfigType() {
        Class<? extends BaseConfig> configType = contributor.getConfigType("MultiTenant");
        assertEquals(BaseConfig.class, configType);
    }

    @Test
    void testGetInstance() {
        KrpcFilter multiTenant = contributor.getInstance("MultiTenant", new BaseConfig(), Mockito.mock(FilterContributorContext.class));
        assertThat(multiTenant).isInstanceOf(MultiTenantTransformationFilter.class);
    }
}