/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaSessionTest {

    private static final String SESSION_ID = "testSession";

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

    @SuppressWarnings("EqualsWithItself") // We are testing the equals impl
    @Test
    void shouldConsiderSameInstanceEqual() {
        // Given
        KafkaSession a = new KafkaSession(SESSION_ID, KafkaSessionState.ESTABLISHING);

        // When
        // Then
        assertThat(a).isEqualTo(a);
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
}