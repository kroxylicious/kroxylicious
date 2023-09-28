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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class ProduceRequestTransformationFilterTest {
    @Test
    void testContributor() {
        ProduceRequestTransformationFilter.Factory factory = new ProduceRequestTransformationFilter.Factory();
        assertThat(factory.configType()).isEqualTo(ProduceRequestTransformationFilter.ProduceRequestTransformationConfig.class);
        assertThatThrownBy(() -> factory.validateConfiguration(null)).isInstanceOf(InvalidFilterConfigurationException.class)
                .hasMessage("ProduceRequestTransformationFilter requires configuration, but config object is null");
        FilterCreationContext constructContext = Mockito.mock(FilterCreationContext.class);
        when(constructContext.getConfig()).thenReturn(
                new ProduceRequestTransformationFilter.ProduceRequestTransformationConfig(ProduceRequestTransformationFilter.UpperCasing.class.getName()));
        assertThat(factory.createFilter(constructContext)).isInstanceOf(ProduceRequestTransformationFilter.class);
    }
}
