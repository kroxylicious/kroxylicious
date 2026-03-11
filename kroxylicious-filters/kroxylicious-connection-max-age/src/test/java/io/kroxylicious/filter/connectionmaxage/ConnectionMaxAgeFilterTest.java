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
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.test.assertj.MockFilterContextAssert;
import io.kroxylicious.test.context.MockFilterContext;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionMaxAgeFilterTest {

    private static final Instant NOW = Instant.parse("2024-01-01T00:00:00Z");
    private static final ZoneId ZONE = ZoneId.of("UTC");

    @Test
    void shouldForwardRequestWhenConnectionIsYoung() {
        // given
        Clock clock = Clock.fixed(NOW, ZONE);
        ConnectionMaxAgeFilter filter = new ConnectionMaxAgeFilter(Duration.ofMinutes(5), clock);
        RequestHeaderData header = new RequestHeaderData();
        ApiVersionsRequestData request = new ApiVersionsRequestData();
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        // when
        CompletionStage<RequestFilterResult> result = filter.onRequest(ApiKeys.API_VERSIONS, (short) 0, header, request, context);

        // then
        assertThat(result).isCompleted();
        MockFilterContextAssert.assertThat(result.toCompletableFuture().join())
                .isForwardRequest()
                .isNotCloseConnection();
    }

    @Test
    void shouldCloseConnectionWhenMaxAgeExceeded() {
        // given - clock at NOW, deadline is NOW + 5 minutes
        Clock creationClock = Clock.fixed(NOW, ZONE);
        ConnectionMaxAgeFilter filter = new ConnectionMaxAgeFilter(Duration.ofMinutes(5), creationClock);

        // advance clock past the deadline
        Clock advancedClock = Clock.fixed(NOW.plus(Duration.ofMinutes(6)), ZONE);
        ConnectionMaxAgeFilter filterWithAdvancedClock = new ConnectionMaxAgeFilter(Duration.ofMinutes(5), advancedClock);
        // We need to test with a filter that sees time past its deadline.
        // Since the clock is injected at construction, we create a filter at NOW and then
        // simulate time advancement by creating a new instance with a mutable clock approach.
        // Instead, let's use a short max age and advanced fixed clock.

        // Simpler approach: create filter with 0-second effective window then check
        // Actually, re-create properly: create filter at T=0 with maxAge=5min, then invoke at T=6min
        // The filter captures deadline at construction (clock.instant() + maxAge).
        // We need a clock that returns NOW at construction, then NOW+6min at invocation.
        // Use a wrapper.

        Clock[] mutableClock = { Clock.fixed(NOW, ZONE) };
        Clock testClock = new Clock() {
            @Override
            public ZoneId getZone() {
                return ZONE;
            }

            @Override
            public Clock withZone(ZoneId zone) {
                return mutableClock[0].withZone(zone);
            }

            @Override
            public Instant instant() {
                return mutableClock[0].instant();
            }
        };

        ConnectionMaxAgeFilter filterUnderTest = new ConnectionMaxAgeFilter(Duration.ofMinutes(5), testClock);

        // advance clock past deadline
        mutableClock[0] = Clock.fixed(NOW.plus(Duration.ofMinutes(6)), ZONE);

        RequestHeaderData header = new RequestHeaderData();
        ApiVersionsRequestData request = new ApiVersionsRequestData();
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        // when
        CompletionStage<RequestFilterResult> result = filterUnderTest.onRequest(ApiKeys.API_VERSIONS, (short) 0, header, request, context);

        // then
        assertThat(result).isCompleted();
        MockFilterContextAssert.assertThat(result.toCompletableFuture().join())
                .isForwardRequest()
                .isCloseConnection();
    }

    @Test
    void shouldNotCloseConnectionWhenExactlyAtDeadline() {
        // given - clock exactly at deadline should not close (isAfter, not isEqual)
        Clock[] mutableClock = { Clock.fixed(NOW, ZONE) };
        Clock testClock = new Clock() {
            @Override
            public ZoneId getZone() {
                return ZONE;
            }

            @Override
            public Clock withZone(ZoneId zone) {
                return mutableClock[0].withZone(zone);
            }

            @Override
            public Instant instant() {
                return mutableClock[0].instant();
            }
        };

        ConnectionMaxAgeFilter filter = new ConnectionMaxAgeFilter(Duration.ofMinutes(5), testClock);

        // set clock exactly at deadline
        mutableClock[0] = Clock.fixed(NOW.plus(Duration.ofMinutes(5)), ZONE);

        RequestHeaderData header = new RequestHeaderData();
        ApiVersionsRequestData request = new ApiVersionsRequestData();
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        // when
        CompletionStage<RequestFilterResult> result = filter.onRequest(ApiKeys.API_VERSIONS, (short) 0, header, request, context);

        // then
        assertThat(result).isCompleted();
        MockFilterContextAssert.assertThat(result.toCompletableFuture().join())
                .isForwardRequest()
                .isNotCloseConnection();
    }

    @Test
    void shouldCloseConnectionOneMillisecondAfterDeadline() {
        // given
        Clock[] mutableClock = { Clock.fixed(NOW, ZONE) };
        Clock testClock = new Clock() {
            @Override
            public ZoneId getZone() {
                return ZONE;
            }

            @Override
            public Clock withZone(ZoneId zone) {
                return mutableClock[0].withZone(zone);
            }

            @Override
            public Instant instant() {
                return mutableClock[0].instant();
            }
        };

        ConnectionMaxAgeFilter filter = new ConnectionMaxAgeFilter(Duration.ofMinutes(5), testClock);

        // set clock 1ms past deadline
        mutableClock[0] = Clock.fixed(NOW.plus(Duration.ofMinutes(5)).plusMillis(1), ZONE);

        RequestHeaderData header = new RequestHeaderData();
        ApiVersionsRequestData request = new ApiVersionsRequestData();
        MockFilterContext context = MockFilterContext.builder(header, request).build();

        // when
        CompletionStage<RequestFilterResult> result = filter.onRequest(ApiKeys.API_VERSIONS, (short) 0, header, request, context);

        // then
        assertThat(result).isCompleted();
        MockFilterContextAssert.assertThat(result.toCompletableFuture().join())
                .isForwardRequest()
                .isCloseConnection();
    }
}
