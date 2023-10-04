/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;

class FilterTest {
    @Test
    void testFactory() {
        ProduceRequestTransformation factory = new ProduceRequestTransformation();
        assertThatThrownBy(() -> factory.initialize(null, null)).isInstanceOf(PluginConfigurationException.class)
                .hasMessage("ProduceRequestTransformation requires configuration, but config object is null");
        FilterFactoryContext constructContext = Mockito.mock(FilterFactoryContext.class);
        doReturn(new UpperCasing()).when(constructContext).pluginInstance(eq(ByteBufferTransformationFactory.class), any());
        ProduceRequestTransformation.Config config = new ProduceRequestTransformation.Config(
                UpperCasing.class.getName(),
                new UpperCasing.Config("UTF-8"));
        assertThat(factory.createFilter(constructContext, config)).isInstanceOf(ProduceRequestTransformation.Filter.class);
    }
}
