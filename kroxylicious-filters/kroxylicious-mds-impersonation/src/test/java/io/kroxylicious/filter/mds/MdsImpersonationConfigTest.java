/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.mds;

import java.net.URI;
import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.Tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MdsImpersonationConfigTest {
    private static final URI ENDPOINT = URI.create("https://mds.example.test/security/1.0/impersonate");
    private final KeyPair key = new KeyPair("proxy.key", "proxy.crt", null);
    private final Tls tls = new Tls(key, null, null, null);

    @ParameterizedTest
    @ValueSource(strings = { "http://mds.example.test/", "https://user:secret@mds.example.test/", "https://mds.example.test/?secret=value",
            "https://mds.example.test/#fragment" })
    void requiresHttpsWithoutEmbeddedCredentials(String url) {
        // Given
        var endpoint = URI.create(url);

        // When
        // Then
        assertThatThrownBy(() -> new MdsImpersonationConfig(endpoint, tls, null, null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void requiresTheProxyIdentityForMds() {
        // Given
        var withoutKey = new Tls(null, null, null, null);

        // When
        // Then
        assertThatThrownBy(() -> new MdsImpersonationConfig(ENDPOINT, withoutKey, null, null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void refusesDisabledMdsCertificateVerification() {
        // Given
        var insecure = new Tls(key, new InsecureTls(true), null, null);

        // When
        // Then
        assertThatThrownBy(() -> new MdsImpersonationConfig(ENDPOINT, insecure, null, null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void defaultsTimeoutAndExpiryMargin() {
        // Given
        var config = new MdsImpersonationConfig(ENDPOINT, tls, null, null);

        // When
        Duration timeout = config.requestTimeout();
        Duration margin = config.expiryMargin();

        // Then
        assertThat(timeout).isEqualTo(Duration.ofSeconds(5));
        assertThat(margin).isEqualTo(Duration.ofSeconds(5));
    }
}
