/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.audit.emitter.slf4j;

import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.audit.AuditEmitter;
import io.kroxylicious.proxy.audit.AuditableAction;

import edu.umd.cs.findbugs.annotations.Nullable;
import nl.altindag.log.LogCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class Slf4jEmitterTest {

    @BeforeAll
    static void checkLoggingConfiguration() {
        // This test depends on certain loggers being at info level
        // Let's test that the logging configuration is compatible with the
        // assertions in the @Test methods
        Logger logger = LoggerFactory.getLogger("audit.Connect");
        logger.warn("Hello World!");
        assertThat(logger.isDebugEnabled()).isFalse();
        assertThat(logger.isInfoEnabled()).isTrue();
        assertThat(logger.isWarnEnabled()).isTrue();
        assertThat(logger.isErrorEnabled()).isTrue();

        logger = LoggerFactory.getLogger("audit.Authenticate");
        assertThat(logger.isDebugEnabled()).isFalse();
        assertThat(logger.isInfoEnabled()).isTrue();
        assertThat(logger.isWarnEnabled()).isTrue();
        assertThat(logger.isErrorEnabled()).isTrue();
    }

    static Stream<Arguments> shouldBeInterestedBasedOnDefaultLevel() {
        return Stream.of(
                Arguments.of(Level.DEBUG, "Connect", null, false),
                Arguments.of(Level.DEBUG, "Authenticate", null, false),
                Arguments.of(Level.INFO, "Connect", null, true),
                Arguments.of(Level.INFO, "Authenticate", null, true),
                Arguments.of(Level.WARN, "Connect", null, true),
                Arguments.of(Level.WARN, "Authenticate", null, true),
                Arguments.of(Level.ERROR, "Connect", null, true),
                Arguments.of(Level.ERROR, "Authenticate", null, true));
    }

    @ParameterizedTest
    @MethodSource
    void shouldBeInterestedBasedOnDefaultLevel(Level defaultLevel,
                                               String action,
                                               @Nullable String status,
                                               boolean expectActionToBeLogged) {
        // Given
        String message = action + " " + (status == null ? "success!" : status);

        AuditableAction auditableAction = mock(AuditableAction.class);
        when(auditableAction.action()).thenReturn(action);
        when(auditableAction.status()).thenReturn(null);
        AuditEmitter.Context context = mock(AuditEmitter.Context.class);
        when(context.asJsonString(any())).thenReturn(message);
        try (LogCaptor connectCaptor = LogCaptor.forName("audit." + action)) {

            try (var emitter = new Slf4jEmitter(defaultLevel, Map.of())) {
                assertThat(emitter.isInterested(action, null))
                        .as("emitter's default is %s, and logger is as INFO", defaultLevel)
                        .isEqualTo(expectActionToBeLogged);

                emitter.emitAction(auditableAction, context);
            }

            if (expectActionToBeLogged) {
                assertThat(connectCaptor.getLogs()).singleElement().isEqualTo(message);
            }
            else {
                assertThat(connectCaptor.getLogs()).isEmpty();
            }

        }

    }

    @Test
    void shouldBeInterestedBasedOnOverriddenLevel() {
        try (var emitter = new Slf4jEmitter(Level.OFF, Map.of(
                new ActionMatch("Connect", false), Level.INFO,
                new ActionMatch("Authenticate", true), Level.WARN,
                new ActionMatch("Authenticate", false), Level.ERROR))) {

            assertThat(emitter.isInterested("Connect", null))
                    .as("emitter's default (OFF) applies, so should not emit")
                    .isFalse();
            assertThat(emitter.isInterested("Connect", "SomeFailure"))
                    .as("matching ActionMatch (INFO) applies, and logger `audit.Authenticate` is at INFO, so should emit")
                    .isTrue();
            assertThat(emitter.isInterested("Authenticate", null))
                    .as("matching ActionMatch (WARN) applies, and logger `audit.Authenticate` is at INFO, so should emit")
                    .isTrue();
            assertThat(emitter.isInterested("Authenticate", "SomeFailure"))
                    .as("matching ActionMatch (ERROR) applies, and logger `audit.Authenticate` is at INFO, so should emit")
                    .isTrue();
        }
    }

}