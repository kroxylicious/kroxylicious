/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaSessionTest {

    @ParameterizedTest
    @EnumSource(KafkaSessionState.class)
    void shouldTransitionTo(KafkaSessionState target) {
        // Given
        KafkaSession kafkaSession = new KafkaSession("testSession", KafkaSessionState.NOT_AUTHENTICATED);

        // When
        KafkaSession actualSession = kafkaSession.in(target);

        // Then
        assertThat(actualSession)
                .satisfies(s -> {
                    assertThat(s.sessionId()).isEqualTo(kafkaSession.sessionId());
                    assertThat(s.currentState()).isEqualTo(target);
                });
    }
}