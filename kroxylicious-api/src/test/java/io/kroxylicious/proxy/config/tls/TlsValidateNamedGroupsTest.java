/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Set;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class TlsValidateNamedGroupsTest {

    private static Tls tlsWithNamedGroups(AllowDeny<String> namedGroups) {
        return new Tls(null, null, null, null, null, namedGroups);
    }

    private static SSLContext sslContextExposing(String... defaultNamedGroups) {
        var params = mock(SSLParameters.class);
        when(params.getNamedGroups()).thenReturn(defaultNamedGroups);
        var sslContext = mock(SSLContext.class);
        when(sslContext.getDefaultSSLParameters()).thenReturn(params);
        return sslContext;
    }

    @Test
    void doesNotProbeWhenNamedGroupsNull() {
        // Given
        var tls = tlsWithNamedGroups(null);

        // When
        var throwable = catchThrowable(tls::validateNamedGroups);

        // Then
        assertThat(throwable).isNull();
    }

    @Test
    void doesNotProbeWhenOnlyAllowedConfigured() {
        // Given
        var tls = tlsWithNamedGroups(new AllowDeny<>(List.of("X25519MLKEM768", "x25519"), null));

        // When
        var throwable = catchThrowable(tls::validateNamedGroups);

        // Then
        assertThat(throwable).isNull();
    }

    @Test
    void doesNotProbeWhenAllowedAndDeniedConfigured() {
        // Given
        var tls = tlsWithNamedGroups(new AllowDeny<>(List.of("X25519MLKEM768"), Set.of("x25519")));

        // When
        var throwable = catchThrowable(tls::validateNamedGroups);

        // Then
        assertThat(throwable).isNull();
    }

    @Test
    void doesNotProbeWhenBothListsEmpty() {
        // Given
        var tls = tlsWithNamedGroups(new AllowDeny<>(List.of(), Set.of()));

        // When
        var throwable = catchThrowable(tls::validateNamedGroups);

        // Then
        assertThat(throwable).isNull();
    }

    @Test
    void doesNotProbeWhenOnlyEmptyAllowedConfigured() {
        // Given
        var tls = tlsWithNamedGroups(new AllowDeny<>(List.of(), null));

        // When
        var throwable = catchThrowable(tls::validateNamedGroups);

        // Then
        assertThat(throwable).isNull();
    }

    @Test
    void deniedOnlyPassesWhenJdkExposesDefaults() {
        // Given
        var tls = tlsWithNamedGroups(new AllowDeny<>(null, Set.of("x25519")));
        var sslContext = sslContextExposing("X25519MLKEM768", "x25519", "secp256r1");

        try (var mocked = mockStatic(SSLContext.class)) {
            mocked.when(SSLContext::getDefault).thenReturn(sslContext);

            // When
            var throwable = catchThrowable(tls::validateNamedGroups);

            // Then
            assertThat(throwable).isNull();
        }
    }

    @Test
    void deniedOnlyFailsWhenJdkReturnsNullDefaults() {
        // Given
        var tls = tlsWithNamedGroups(new AllowDeny<>(null, Set.of("x25519")));
        var sslContext = sslContextExposing((String[]) null);

        try (var mocked = mockStatic(SSLContext.class)) {
            mocked.when(SSLContext::getDefault).thenReturn(sslContext);

            // When
            var throwable = catchThrowable(tls::validateNamedGroups);

            // Then
            assertThat(throwable)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("does not expose its default named groups");
        }
    }

    @Test
    void deniedOnlyFailsWhenJdkReturnsEmptyDefaults() {
        // Given
        var tls = tlsWithNamedGroups(new AllowDeny<>(null, Set.of("x25519")));
        var sslContext = sslContextExposing();

        try (var mocked = mockStatic(SSLContext.class)) {
            mocked.when(SSLContext::getDefault).thenReturn(sslContext);

            // When
            var throwable = catchThrowable(tls::validateNamedGroups);

            // Then
            assertThat(throwable)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("does not expose its default named groups");
        }
    }

    @Test
    void deniedOnlyWrapsNoSuchAlgorithmException() {
        // Given
        var tls = tlsWithNamedGroups(new AllowDeny<>(null, Set.of("x25519")));

        try (var mocked = mockStatic(SSLContext.class)) {
            mocked.when(SSLContext::getDefault).thenThrow(new NoSuchAlgorithmException("boom"));

            // When
            var throwable = catchThrowable(tls::validateNamedGroups);

            // Then
            assertThat(throwable)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Unable to obtain the platform default SSL parameters")
                    .hasCauseInstanceOf(NoSuchAlgorithmException.class);
        }
    }

}
