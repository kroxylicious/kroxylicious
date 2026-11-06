/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.net.InetSocketAddress;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;

import io.kroxylicious.proxy.internal.net.HaProxyContext;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaSessionTest {

    private static final String SESSION_ID = "testSession";
    private static final HaProxyContext TCP4_HA_PROXY_CONTEXT = new HaProxyContext(HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
            "192.0.2.1", "192.0.2.2", 12345, 9092, Map.of());

    @ParameterizedTest
    @EnumSource(KafkaSessionState.class)
    void shouldTransitionTo(KafkaSessionState target) {
        // Given
        KafkaSession kafkaSession = new KafkaSession(SESSION_ID, KafkaSessionState.NOT_AUTHENTICATED);

        // When
        KafkaSession actualSession = kafkaSession.transitionTo(target);

        // Then
        assertThat(actualSession)
                .isSameAs(kafkaSession)
                .satisfies(s -> {
                    assertThat(s.sessionId()).isEqualTo(kafkaSession.sessionId());
                    assertThat(s.currentState()).isEqualTo(target);
                });
    }

    @Test
    void shouldAllocateSessionId() {
        // Given

        // When
        KafkaSession kafkaSession = new KafkaSession(null, KafkaSessionState.NOT_AUTHENTICATED);

        // Then
        assertThat(kafkaSession.sessionId()).isNotNull();
    }

    @Test
    void shouldConsiderEqualObjectEqual() {
        // Given
        KafkaSession a = new KafkaSession(SESSION_ID, KafkaSessionState.ESTABLISHING);
        KafkaSession b = new KafkaSession(SESSION_ID, KafkaSessionState.ESTABLISHING);

        // When
        // Then
        assertThat(a).isEqualTo(b);
    }

    @Test
    void shouldConsiderSessionsWithDifferentIdsNotEqual() {
        // Given
        KafkaSession a = new KafkaSession(SESSION_ID, KafkaSessionState.ESTABLISHING);
        KafkaSession b = new KafkaSession("session2", KafkaSessionState.ESTABLISHING);

        // When
        // Then
        assertThat(a).isNotEqualTo(b);
    }

    @SuppressWarnings("AssertBetweenInconvertibleTypes") // testing that the equals method handles inconvertible types
    @Test
    void shouldConsiderDifferentTypesAsNotEqual() {
        // Given
        KafkaSession a = new KafkaSession(SESSION_ID, KafkaSessionState.ESTABLISHING);

        // When
        // Then
        assertThat(a).isNotEqualTo("Wibble");
    }

    @Test
    void shouldConsiderNullAsNotEqual() {
        // Given
        KafkaSession a = new KafkaSession(SESSION_ID, KafkaSessionState.ESTABLISHING);

        // When
        // Then
        assertThat(a).isNotEqualTo(null);
    }

    @Test
    void shouldConsiderSessionsWithDifferentStatesNotEqual() {
        // Given
        KafkaSession a = new KafkaSession(SESSION_ID, KafkaSessionState.ESTABLISHING);
        KafkaSession b = new KafkaSession(SESSION_ID, KafkaSessionState.SASL_AUTHENTICATED);

        // When
        // Then
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    void shouldConsiderGenerateSameHashCodeForIdenticalObjects() {
        // Given
        KafkaSession a = new KafkaSession(SESSION_ID, KafkaSessionState.ESTABLISHING);
        KafkaSession b = new KafkaSession(SESSION_ID, KafkaSessionState.ESTABLISHING);

        // When
        // Then
        assertThat(a).hasSameHashCodeAs(b);
    }

    @Test
    void shouldGiveSessionsWithDifferentIdsDifferentHashCode() {
        // Given
        KafkaSession a = new KafkaSession(SESSION_ID, KafkaSessionState.ESTABLISHING);
        KafkaSession b = new KafkaSession("session2", KafkaSessionState.ESTABLISHING);

        // When
        // Then
        assertThat(a).doesNotHaveSameHashCodeAs(b);
    }

    @Test
    void shouldGiveSessionsWithDifferentStatesDifferentHashCode() {
        // Given
        KafkaSession a = new KafkaSession(SESSION_ID, KafkaSessionState.ESTABLISHING);
        KafkaSession b = new KafkaSession(SESSION_ID, KafkaSessionState.SASL_AUTHENTICATED);

        // When
        // Then
        assertThat(a).doesNotHaveSameHashCodeAs(b);
    }

    @Test
    void shouldHaveNoProxyProtocolContextInitially() {
        // Given
        KafkaSession kafkaSession = new KafkaSession(SESSION_ID, KafkaSessionState.ESTABLISHING);

        // When
        // Then
        assertThat(kafkaSession.haProxyContext()).isNull();
        assertThat(kafkaSession.clientProxyProtocolContext()).isEmpty();
    }

    @Test
    void shouldHaveProxyProtocolContextOnceSet() {
        // Given
        KafkaSession kafkaSession = new KafkaSession(SESSION_ID, KafkaSessionState.ESTABLISHING);

        // When
        kafkaSession.setHaProxyContext(TCP4_HA_PROXY_CONTEXT);

        // Then
        assertThat(kafkaSession.haProxyContext()).isEqualTo(TCP4_HA_PROXY_CONTEXT);
        assertThat(kafkaSession.clientProxyProtocolContext()).hasValueSatisfying(context -> {
            assertThat(context.sourceAddress()).isEqualTo(new InetSocketAddress("192.0.2.1", 12345));
            assertThat(context.destinationAddress()).isEqualTo(new InetSocketAddress("192.0.2.2", 9092));
        });
    }
}