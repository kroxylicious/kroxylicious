/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.config.BaseConfig;
import io.kroxylicious.proxy.filter.FilterContributorContext;
import io.kroxylicious.proxy.filter.KrpcFilter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FilterContributorManagerTest {

    public static final FilterContributorManager INSTANCE = FilterContributorManager.getInstance();

    @Test
    void testGetConfigType() {
        Class<? extends BaseConfig> configType = INSTANCE.getConfigType(TestFilterContributor.SHORT_NAME);
        assertThat(configType).isEqualTo(Config.class);
    }

    @Test
    void testGetConfigTypeFailsIfNoMatches() {
        assertThatThrownBy(() -> INSTANCE.getConfigType("mismatch")).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No filter found for name 'mismatch'");
    }

    @Test
    void testGetFilter() {
        FilterContributorContext mock = Mockito.mock(FilterContributorContext.class);
        Config config = new Config();
        KrpcFilter krpcFilter = INSTANCE.getFilter(TestFilterContributor.SHORT_NAME, config, mock);
        assertThat(krpcFilter).isInstanceOf(TestKrpcFilter.class);
        TestKrpcFilter testKrpcFilter = ((TestKrpcFilter) krpcFilter);
        assertThat(testKrpcFilter.shortName()).isEqualTo(TestFilterContributor.SHORT_NAME);
        assertThat(testKrpcFilter.config()).isSameAs(config);
        assertThat(testKrpcFilter.context()).isSameAs(mock);
    }

    @Test
    void testGetFilterFailsIfNoMatch() {
        FilterContributorContext mock = Mockito.mock(FilterContributorContext.class);
        Config config = new Config();
        assertThatThrownBy(() -> INSTANCE.getFilter("mismatch", config, mock)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No filter found for name 'mismatch'");
    }

}