/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.InvalidFilterConfigurationException;
import io.kroxylicious.proxy.internal.filter.FetchResponseTransformationFilter.FetchResponseTransformationConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FetchResponseTransformationFilterTest {

    @Test
    void testContributor() {
        FetchResponseTransformationFilterFactory factory = new FetchResponseTransformationFilterFactory();
        assertThat(factory.configType()).isEqualTo(FetchResponseTransformationConfig.class);
        assertThatThrownBy(() -> factory.validateConfiguration(null)).isInstanceOf(InvalidFilterConfigurationException.class)
                .hasMessage("FetchResponseTransformationFilter requires configuration, but config object is null");
        FilterCreationContext constructContext = Mockito.mock(FilterCreationContext.class);
        FetchResponseTransformationConfig config = new FetchResponseTransformationConfig(ProduceRequestTransformationFilter.UpperCasing.class.getName());
        assertThat(factory.createFilter(constructContext, config)).isInstanceOf(FetchResponseTransformationFilter.class);
    }

}
