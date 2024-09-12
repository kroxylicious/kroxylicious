/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FilterTest {
    private final RequiresConfigFactory factory = new RequiresConfigFactory();
    @Mock
    FilterFactoryContext constructContext;

    @Test
    void detectsMissingConfig() {
        assertThatThrownBy(() -> factory.initialize(null, null)).isInstanceOf(PluginConfigurationException.class)
                                                                .hasMessage(
                                                                        RequiresConfigFactory.class.getSimpleName()
                                                                            + " requires configuration, but config object is null"
                                                                );
    }

    @Test
    void createFilter() {
        var config = new ExampleConfig();
        assertThat(factory.createFilter(constructContext, config)).isInstanceOf(RequiresConfigFactory.Filter.class);
    }
}
