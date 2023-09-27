/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.InvalidConfigurationException;
import io.kroxylicious.proxy.filter.FilterConstructContext;
import io.kroxylicious.proxy.internal.filter.FetchResponseTransformationFilter.Factory;
import io.kroxylicious.proxy.internal.filter.FetchResponseTransformationFilter.FetchResponseTransformationConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class FetchResponseTransformationFilterTest {

    @Test
    void testContributor() {
        Factory factory = new Factory();
        assertThat(factory.getConfigType()).isEqualTo(FetchResponseTransformationConfig.class);
        assertThatThrownBy(() -> factory.validateConfiguration(null)).isInstanceOf(InvalidConfigurationException.class)
                .hasMessage("FetchResponseTransformationFilter requires configuration, but config object is null");
        FilterConstructContext constructContext = Mockito.mock(FilterConstructContext.class);
        when(constructContext.getConfig()).thenReturn(new FetchResponseTransformationConfig(ProduceRequestTransformationFilter.UpperCasing.class.getName()));
        assertThat(factory.createInstance(constructContext)).isInstanceOf(FetchResponseTransformationFilter.class);
    }

}
