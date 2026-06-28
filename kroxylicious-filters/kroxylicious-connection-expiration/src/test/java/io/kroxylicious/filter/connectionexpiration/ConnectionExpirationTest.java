/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.connectionexpiration;

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
import io.kroxylicious.testing.filter.assertj.MockFilterContextAssert;
import io.kroxylicious.testing.filter.context.MockFilterContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class ConnectionExpirationTest {

    private static final Instant NOW = Instant.parse("2024-01-01T00:00:00Z");
    private static final ZoneId ZONE = ZoneId.of("UTC");

    @Mock
    private FilterFactoryContext context;

    @Test
    void shouldInitializeWithValidConfig() {
        ConnectionExpiration factory = new ConnectionExpiration();
        ConnectionExpirationFilterConfig config = new ConnectionExpirationFilterConfig(Duration.ofSeconds(300), null);

        ConnectionExpirationFilterConfig result = factory.initialize(context, config);

        assertThat(result).isSameAs(config);
    }

    @Test
    void shouldRejectNullConfig() {
        ConnectionExpiration factory = new ConnectionExpiration();

        assertThatThrownBy(() -> factory.initialize(context, null))
                .isInstanceOf(PluginConfigurationException.class);
    }

    @Test
    void shouldCreateFilter() {
        ConnectionExpiration factory = new ConnectionExpiration(Clock.fixed(NOW, ZONE));
        ConnectionExpirationFilterConfig config = new ConnectionExpirationFilterConfig(Duration.ofSeconds(300), null);
        factory.initialize(context, config);

        var filter = factory.createFilter(context, config);

        assertThat(filter).isInstanceOf(ConnectionExpirationFilter.class);
    }

    @Test
    void shouldCreateFilterWithJitter() {
        ConnectionExpiration factory = new ConnectionExpiration(Clock.fixed(NOW, ZONE));
        ConnectionExpirationFilterConfig config = new ConnectionExpirationFilterConfig(Duration.ofSeconds(300), Duration.ofSeconds(30));
        factory.initialize(context, config);

        var filter = factory.createFilter(context, config);

        assertThat(filter).isInstanceOf(ConnectionExpirationFilter.class);
    }

    @Test
    void shouldApplyPositiveJitterToEffectiveMaxAge() {
        MutableClock mutableClock = MutableClock.of(NOW, ZONE);
        Duration maxAge = Duration.ofSeconds(300);
        Duration jitter = Duration.ofSeconds(30);
        ConnectionExpiration factory = new ConnectionExpiration(mutableClock, (origin, bound) -> jitter.toMillis());
        ConnectionExpirationFilterConfig config = new ConnectionExpirationFilterConfig(maxAge, jitter);
        factory.initialize(context, config);

        ConnectionExpirationFilter filter = (ConnectionExpirationFilter) factory.createFilter(context, config);
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
        ConnectionExpiration factory = new ConnectionExpiration(mutableClock, (origin, bound) -> -jitter.toMillis());
        ConnectionExpirationFilterConfig config = new ConnectionExpirationFilterConfig(maxAge, jitter);
        factory.initialize(context, config);

        ConnectionExpirationFilter filter = (ConnectionExpirationFilter) factory.createFilter(context, config);

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
        ConnectionExpiration factory = new ConnectionExpiration(mutableClock, (origin, bound) -> -jitter.toMillis());
        ConnectionExpirationFilterConfig config = new ConnectionExpirationFilterConfig(maxAge, jitter);
        factory.initialize(context, config);

        ConnectionExpirationFilter filter = (ConnectionExpirationFilter) factory.createFilter(context, config);
        mutableClock.set(NOW.plusMillis(2));

        RequestHeaderData header = new RequestHeaderData();
        ApiVersionsRequestData request = new ApiVersionsRequestData();
        MockFilterContext ctx = MockFilterContext.builder(header, request).build();

        assertThat(filter.onRequest(ApiKeys.API_VERSIONS, (short) 0, header, request, ctx))
                .succeedsWithin(Duration.ZERO).satisfies(r -> MockFilterContextAssert.assertThat(r).isCloseConnection());
    }
}
