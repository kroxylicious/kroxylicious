/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.auth;

import java.time.Instant;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class BearerTokenTest {

    private static final String TOKEN = "abcdef";

    static Stream<Arguments> constructorArgsRequired() {
        return Stream.of(argumentSet("token required", (Runnable) () -> new BearerToken(null, Instant.MAX, Instant.MAX), "token"),
                argumentSet("created required", (Runnable) () -> new BearerToken(TOKEN, null, Instant.MAX), "created"),
                argumentSet("expires required", (Runnable) () -> new BearerToken(TOKEN, Instant.MAX, null), "expires"));
    }

    @MethodSource
    @ParameterizedTest
    void constructorArgsRequired(Runnable runnable, String requiredArg) {
        Assertions.assertThatThrownBy(runnable::run).isInstanceOf(NullPointerException.class)
                .hasMessage(requiredArg + " is required");
    }

    @Test
    void toStringRedactsToken() {
        BearerToken token = new BearerToken(TOKEN, Instant.MAX, Instant.MAX);
        assertThat(token.toString()).doesNotContain(TOKEN);
    }

    static Stream<Arguments> isExpired() {
        Instant expiry = Instant.parse("2007-12-03T10:15:30.00Z");
        Instant oneNanoBefore = expiry.minusNanos(1L);
        Instant oneNanoAfter = expiry.plusNanos(1L);
        return Stream.of(argumentSet("exactly at expiry", expiry, expiry, false),
                argumentSet("before expiry", expiry, oneNanoBefore, false),
                argumentSet("after expiry", expiry, oneNanoAfter, true));
    }

    @MethodSource
    @ParameterizedTest
    void isExpired(Instant expires, Instant now, boolean expectedExpired) {
        BearerToken token = new BearerToken(TOKEN, Instant.MAX, expires);
        assertThat(token.isExpired(now)).isEqualTo(expectedExpired);
    }

}