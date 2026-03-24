/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.connectionmaxage;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.threeten.extra.MutableClock;

import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.test.assertj.MockFilterContextAssert;
import io.kroxylicious.test.context.MockFilterContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class ConnectionMaxAgeFilterFactoryTest {

    private static final Instant NOW = Instant.parse("2024-01-01T00:00:00Z");
    private static final ZoneId ZONE = ZoneId.of("UTC");

    @Mock
    private FilterFactoryContext context;

    @Test
    void shouldInitializeWithValidConfig() {
        ConnectionMaxAgeFilterFactory factory = new ConnectionMaxAgeFilterFactory();
        ConnectionMaxAgeFilterConfig config = new ConnectionMaxAgeFilterConfig(Duration.ofSeconds(300), null);

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
        ConnectionMaxAgeFilterFactory factory = new ConnectionMaxAgeFilterFactory(Clock.fixed(NOW, ZONE));
        ConnectionMaxAgeFilterConfig config = new ConnectionMaxAgeFilterConfig(Duration.ofSeconds(300), null);
        factory.initialize(context, config);

        var filter = factory.createFilter(context, config);

        assertThat(filter).isInstanceOf(ConnectionMaxAgeFilter.class);
    }

    @Test
    void shouldCreateFilterWithJitter() {
        ConnectionMaxAgeFilterFactory factory = new ConnectionMaxAgeFilterFactory(Clock.fixed(NOW, ZONE));
        ConnectionMaxAgeFilterConfig config = new ConnectionMaxAgeFilterConfig(Duration.ofSeconds(300), Duration.ofSeconds(30));
        factory.initialize(context, config);

        var filter = factory.createFilter(context, config);

        assertThat(filter).isInstanceOf(ConnectionMaxAgeFilter.class);
    }

    @Test
    void shouldApplyPositiveJitterToEffectiveMaxAge() {
        MutableClock mutableClock = MutableClock.of(NOW, ZONE);
        Duration maxAge = Duration.ofSeconds(300);
        Duration jitter = Duration.ofSeconds(30);
        ConnectionMaxAgeFilterFactory factory = new ConnectionMaxAgeFilterFactory(mutableClock, (origin, bound) -> jitter.toMillis());
        ConnectionMaxAgeFilterConfig config = new ConnectionMaxAgeFilterConfig(maxAge, jitter);
        factory.initialize(context, config);

        ConnectionMaxAgeFilter filter = (ConnectionMaxAgeFilter) factory.createFilter(context, config);
        mutableClock.set(NOW.plus(maxAge).plus(jitter).plusMillis(1));

        RequestHeaderData header = new RequestHeaderData();
        ApiVersionsRequestData request = new ApiVersionsRequestData();
        MockFilterContext ctx = MockFilterContext.builder(header, request).build();

        assertThat(filter.onRequest(ApiKeys.API_VERSIONS, (short) 0, header, request, ctx))
                .succeedsWithin(Duration.ZERO).satisfies(r -> MockFilterContextAssert.assertThat(r).isCloseConnection());
    }

    @Test
    void shouldApplyNegativeJitterToEffectiveMaxAge() {
        MutableClock mutableClock = MutableClock.of(NOW, ZONE);
        Duration maxAge = Duration.ofSeconds(300);
        Duration jitter = Duration.ofSeconds(30);
        ConnectionMaxAgeFilterFactory factory = new ConnectionMaxAgeFilterFactory(mutableClock, (origin, bound) -> -jitter.toMillis());
        ConnectionMaxAgeFilterConfig config = new ConnectionMaxAgeFilterConfig(maxAge, jitter);
        factory.initialize(context, config);

        ConnectionMaxAgeFilter filter = (ConnectionMaxAgeFilter) factory.createFilter(context, config);

        mutableClock.set(NOW.plus(maxAge).minus(jitter));
        RequestHeaderData header = new RequestHeaderData();
        ApiVersionsRequestData request = new ApiVersionsRequestData();
        MockFilterContext ctxBefore = MockFilterContext.builder(header, request).build();
        assertThat(filter.onRequest(ApiKeys.API_VERSIONS, (short) 0, header, request, ctxBefore))
                .succeedsWithin(Duration.ZERO).satisfies(r -> MockFilterContextAssert.assertThat(r).isNotCloseConnection());

        mutableClock.set(NOW.plus(maxAge).minus(jitter).plusMillis(1));
        MockFilterContext ctxAfter = MockFilterContext.builder(header, request).build();
        assertThat(filter.onRequest(ApiKeys.API_VERSIONS, (short) 0, header, request, ctxAfter))
                .succeedsWithin(Duration.ZERO).satisfies(r -> MockFilterContextAssert.assertThat(r).isCloseConnection());
    }

    @Test
    void shouldClampEffectiveMaxAgeToOneMillisecond() {
        MutableClock mutableClock = MutableClock.of(NOW, ZONE);
        Duration maxAge = Duration.ofSeconds(300);
        Duration jitter = Duration.ofSeconds(300);
        ConnectionMaxAgeFilterFactory factory = new ConnectionMaxAgeFilterFactory(mutableClock, (origin, bound) -> -jitter.toMillis());
        ConnectionMaxAgeFilterConfig config = new ConnectionMaxAgeFilterConfig(maxAge, jitter);
        factory.initialize(context, config);

        ConnectionMaxAgeFilter filter = (ConnectionMaxAgeFilter) factory.createFilter(context, config);
        mutableClock.set(NOW.plusMillis(2));

        RequestHeaderData header = new RequestHeaderData();
        ApiVersionsRequestData request = new ApiVersionsRequestData();
        MockFilterContext ctx = MockFilterContext.builder(header, request).build();

        assertThat(filter.onRequest(ApiKeys.API_VERSIONS, (short) 0, header, request, ctx))
                .succeedsWithin(Duration.ZERO).satisfies(r -> MockFilterContextAssert.assertThat(r).isCloseConnection());
    }
}
