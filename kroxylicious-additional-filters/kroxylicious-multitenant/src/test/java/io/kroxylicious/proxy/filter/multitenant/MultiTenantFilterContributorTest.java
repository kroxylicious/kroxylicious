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
import io.kroxylicious.proxy.service.ConfigurationDefinition;

import static org.assertj.core.api.Assertions.assertThat;

class MultiTenantFilterContributorTest {

    @Test
    void testGetConfigType() {
        MultiTenantFilterContributor contributor = new MultiTenantFilterContributor();
        Class<? extends BaseConfig> configType = contributor.getConfigType("MultiTenant");
        assertThat(configType).isEqualTo(BaseConfig.class);
    }

    @Test
    void testGetConfigTypeViaConfigurationDefinition() {
        MultiTenantFilterContributor contributor = new MultiTenantFilterContributor();
        ConfigurationDefinition actualConfigurationDefinition = contributor.getConfigDefinition("MultiTenant");
        assertThat(actualConfigurationDefinition).isNotNull().hasFieldOrPropertyWithValue("configurationType", BaseConfig.class);
    }

    @Test
    void testGetInstance() {
        MultiTenantFilterContributor contributor = new MultiTenantFilterContributor();
        Filter filter = contributor.getInstance("MultiTenant", Mockito.mock(FilterConstructContext.class));
        assertThat(filter).isNotNull().isInstanceOf(MultiTenantTransformationFilter.class);
    }

}