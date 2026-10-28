/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import java.util.List;
import java.util.Set;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import org.junit.jupiter.api.Test;

import io.netty.handler.ssl.SslContext;

import io.kroxylicious.proxy.config.IllegalConfigurationException;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.Tls;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaProxyDelegatingSslContextTest {

    private static final String CONTEXT_ID = "Virtual cluster 'demo', gateway 'test'";

    // a distinctive default set, sharing no entries with the allowed/denied values used by the tests, so an assertion
    // that it survives unchanged cannot be satisfied by the configured groups leaking through
    private static final List<String> JDK_DEFAULT_NAMED_GROUPS = List.of("ffdhe2048", "ffdhe3072", "x448");

    private static KafkaProxyDelegatingSslContext contextWithNamedGroups(final AllowDeny<String> namedGroups) {
        var delegate = mock(SslContext.class);
        return new KafkaProxyDelegatingSslContext(delegate, CONTEXT_ID)
                .setNamedGroups(new Tls(null, null, null, null, null, namedGroups));
    }

    private static SSLEngine engineReporting(final String... currentNamedGroups) {
        var params = new SSLParameters();
        if (currentNamedGroups.length > 0) {
            params.setNamedGroups(currentNamedGroups);
        }
        var engine = mock(SSLEngine.class);
        when(engine.getSSLParameters()).thenReturn(params);
        return engine;
    }

    private static SSLEngine engineReportingJdkDefaults() {
        return engineReporting(JDK_DEFAULT_NAMED_GROUPS.toArray(new String[0]));
    }

    @Test
    void leavesParametersUntouchedWhenNoNamedGroupsConfigured() {
        // Given
        var context = contextWithNamedGroups(null);
        var engine = engineReportingJdkDefaults();

        // When
        context.initEngine(engine);

        // Then
        verify(engine).setSSLParameters(engine.getSSLParameters());
        assertThat(engine.getSSLParameters().getNamedGroups()).containsExactlyElementsOf(JDK_DEFAULT_NAMED_GROUPS);
    }

    @Test
    void leavesParametersUntouchedWhenBothListsNull() {
        // Given
        var context = contextWithNamedGroups(new AllowDeny<>(null, null));
        var engine = engineReportingJdkDefaults();

        // When
        context.initEngine(engine);

        // Then
        assertThat(engine.getSSLParameters().getNamedGroups()).containsExactlyElementsOf(JDK_DEFAULT_NAMED_GROUPS);
    }

    @Test
    void appliesAllowedGroupsInConfiguredOrder() {
        // Given
        var context = contextWithNamedGroups(new AllowDeny<>(List.of("X25519MLKEM768", "secp256r1", "X25519"), null));
        var engine = engineReportingJdkDefaults();

        // When
        context.initEngine(engine);

        // Then
        assertThat(engine.getSSLParameters().getNamedGroups())
                .containsExactly("X25519MLKEM768", "secp256r1", "X25519");
    }

    @Test
    void prunesDeniedGroupsFromAllowedCaseInsensitively() {
        // Given
        // denies the first entry (case-insensitively) and a middle entry, and the near-miss 'X25519' must not remove 'X25519MLKEM768'
        var context = contextWithNamedGroups(new AllowDeny<>(
                List.of("x25519", "X25519MLKEM768", "secp256r1", "secp384r1"), Set.of("X25519", "secp256r1")));
        var engine = engineReportingJdkDefaults();

        // When
        context.initEngine(engine);

        // Then
        assertThat(engine.getSSLParameters().getNamedGroups()).containsExactly("X25519MLKEM768", "secp384r1");
    }

    @Test
    void prunesDeniedGroupsFromJdkDefaultsWhenOnlyDeniedConfigured() {
        // Given
        // denies the first default (case-insensitively) and the last default
        var context = contextWithNamedGroups(new AllowDeny<>(null, Set.of("x25519mlkem768", "secp384r1")));
        var engine = engineReporting("X25519MLKEM768", "x25519", "secp256r1", "secp384r1");

        // When
        context.initEngine(engine);

        // Then
        assertThat(engine.getSSLParameters().getNamedGroups()).containsExactly("x25519", "secp256r1");
    }

    @Test
    void appliesEmptyGroupsWhenEverythingAllowedIsDenied() {
        // Given
        var context = contextWithNamedGroups(new AllowDeny<>(List.of("x25519"), Set.of("x25519")));
        var engine = engineReportingJdkDefaults();

        // When
        context.initEngine(engine);

        // Then
        assertThat(engine.getSSLParameters().getNamedGroups()).isEmpty();
    }

    @Test
    void throwsWhenOnlyDeniedConfiguredAndJdkExposesNoDefaults() {
        // Given
        var context = contextWithNamedGroups(new AllowDeny<>(null, Set.of("x25519")));
        var engine = engineReporting();

        // When
        var throwable = catchThrowable(() -> context.initEngine(engine));

        // Then
        assertThat(throwable)
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(CONTEXT_ID)
                .hasMessageContaining("does not expose its default named groups");
    }

}
