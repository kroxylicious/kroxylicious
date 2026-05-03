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
import org.threeten.extra.MutableClock;

import io.kroxylicious.testing.filter.assertj.MockFilterContextAssert;
import io.kroxylicious.testing.filter.context.MockFilterContext;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionExpirationFilterTest {

    private static final Instant NOW = Instant.parse("2024-01-01T00:00:00Z");
    private static final ZoneId ZONE = ZoneId.of("UTC");

    @Test
    void shouldForwardRequestWhenConnectionIsYoung() {
        Clock clock = Clock.fixed(NOW, ZONE);
        ConnectionExpirationFilter filter = new ConnectionExpirationFilter(Duration.ofMinutes(5), clock);
        RequestHeaderData header = new RequestHeaderData();
        ApiVersionsRequestData request = new ApiVersionsRequestData();
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        var result = filter.onRequest(ApiKeys.API_VERSIONS, (short) 0, header, request, context);

        assertThat(result).succeedsWithin(Duration.ZERO).satisfies(r -> MockFilterContextAssert.assertThat(r).isForwardRequest()
                .isNotCloseConnection());
    }

    @Test
    void shouldCloseConnectionWhenExpirationExceeded() {
        MutableClock mutableClock = MutableClock.of(NOW, ZONE);
        ConnectionExpirationFilter filterUnderTest = new ConnectionExpirationFilter(Duration.ofMinutes(5), mutableClock);
        mutableClock.set(NOW.plus(Duration.ofMinutes(6)));

        RequestHeaderData header = new RequestHeaderData();
        ApiVersionsRequestData request = new ApiVersionsRequestData();
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        var result = filterUnderTest.onRequest(ApiKeys.API_VERSIONS, (short) 0, header, request, context);

        assertThat(result).succeedsWithin(Duration.ZERO).satisfies(r -> MockFilterContextAssert.assertThat(r).isForwardRequest()
                .isCloseConnection());
    }

    @Test
    void shouldNotCloseConnectionWhenExactlyAtDeadline() {
        MutableClock mutableClock = MutableClock.of(NOW, ZONE);
        ConnectionExpirationFilter filter = new ConnectionExpirationFilter(Duration.ofMinutes(5), mutableClock);
        mutableClock.set(NOW.plus(Duration.ofMinutes(5)));

        RequestHeaderData header = new RequestHeaderData();
        ApiVersionsRequestData request = new ApiVersionsRequestData();
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        var result = filter.onRequest(ApiKeys.API_VERSIONS, (short) 0, header, request, context);

        assertThat(result).succeedsWithin(Duration.ZERO).satisfies(r -> MockFilterContextAssert.assertThat(r).isForwardRequest()
                .isNotCloseConnection());
    }

    @Test
    void shouldCloseConnectionOneMillisecondAfterDeadline() {
        MutableClock mutableClock = MutableClock.of(NOW, ZONE);
        ConnectionExpirationFilter filter = new ConnectionExpirationFilter(Duration.ofMinutes(5), mutableClock);
        mutableClock.set(NOW.plus(Duration.ofMinutes(5)).plusMillis(1));

        RequestHeaderData header = new RequestHeaderData();
        ApiVersionsRequestData request = new ApiVersionsRequestData();
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        var result = filter.onRequest(ApiKeys.API_VERSIONS, (short) 0, header, request, context);

        assertThat(result).succeedsWithin(Duration.ZERO).satisfies(r -> MockFilterContextAssert.assertThat(r).isForwardRequest()
                .isCloseConnection());
    }
}
