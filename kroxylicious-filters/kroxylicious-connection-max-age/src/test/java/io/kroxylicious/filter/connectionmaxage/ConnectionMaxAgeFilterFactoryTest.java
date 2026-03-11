/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.connectionmaxage;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class ConnectionMaxAgeFilterFactoryTest {

    @Mock
    private FilterFactoryContext context;

    @Test
    void shouldInitializeWithValidConfig() {
        ConnectionMaxAgeFilterFactory factory = new ConnectionMaxAgeFilterFactory();
        ConnectionMaxAgeFilterConfig config = new ConnectionMaxAgeFilterConfig(300, null);

        ConnectionMaxAgeFilterConfig result = factory.initialize(context, config);

        assertThat(result).isSameAs(config);
    }

    @Test
    void shouldRejectNullConfig() {
        ConnectionMaxAgeFilterFactory factory = new ConnectionMaxAgeFilterFactory();

        assertThatThrownBy(() -> factory.initialize(context, null))
                .isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void shouldCreateFilter() {
        Clock clock = Clock.fixed(Instant.parse("2024-01-01T00:00:00Z"), ZoneId.of("UTC"));
        ConnectionMaxAgeFilterFactory factory = new ConnectionMaxAgeFilterFactory(clock);
        ConnectionMaxAgeFilterConfig config = new ConnectionMaxAgeFilterConfig(300, null);
        factory.initialize(context, config);

        var filter = factory.createFilter(context, config);

        assertThat(filter).isInstanceOf(ConnectionMaxAgeFilter.class);
    }

    @Test
    void shouldCreateFilterWithJitter() {
        Clock clock = Clock.fixed(Instant.parse("2024-01-01T00:00:00Z"), ZoneId.of("UTC"));
        ConnectionMaxAgeFilterFactory factory = new ConnectionMaxAgeFilterFactory(clock);
        ConnectionMaxAgeFilterConfig config = new ConnectionMaxAgeFilterConfig(300, 30L);
        factory.initialize(context, config);

        var filter = factory.createFilter(context, config);

        assertThat(filter).isInstanceOf(ConnectionMaxAgeFilter.class);
    }
}
