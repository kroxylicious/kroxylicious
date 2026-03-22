/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.kroxylicious.proxy.audit.AuditEmitter;
import io.kroxylicious.proxy.audit.AuditLogger;
import io.kroxylicious.proxy.audit.AuditableAction;
import io.kroxylicious.proxy.audit.AuditableActionBuilder;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class AuditLoggerImplTest {

    public final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    @Mock
    ProxyChannelStateMachine pcsm;

    private void mockedProxyChannelStateMachine() {
        doReturn(InetSocketAddress.createUnresolved("127.0.0.1", 12345)).when(pcsm).clientAddress();
        doReturn(new Subject(new User("alice"))).when(pcsm).authenticatedSubject();
        doReturn("ABC-1234").when(pcsm).sessionId();
    }

    @Test
    void shouldLogSuccessfulActionWhenNoEmitters() {
        // Given
        var pcsm = mock(ProxyChannelStateMachine.class);
        var logger = new AuditLoggerImpl(List.of(),
                () -> ClientActorImpl.of(pcsm.clientAddress(),
                        pcsm.sessionId(),
                        pcsm.authenticatedSubject()),
                Instant::now);

        // when
        logger.action("Foo")
                .withObjectRef(Map.of("vc", "my-virtual-cluster"))
                .addToContext("A", "a")
                .log();
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldOnlyCallEmitActionIfEmitterIsInterested(boolean interested) {
        // Given
        if (interested) {
            mockedProxyChannelStateMachine();
        }

        var emitter = mock(AuditEmitter.class);
        doReturn(interested).when(emitter).isInterested(any(), any());

        var logger = new AuditLoggerImpl(List.of(emitter),
                () -> ClientActorImpl.of(pcsm.clientAddress(),
                        pcsm.sessionId(),
                        pcsm.authenticatedSubject()),
                Instant::now);

        // when
        logger.action("Foo")
                .withObjectRef(Map.of("vc", "my-virtual-cluster"))
                .addToContext("A", "a")
                .log();

        // then
        verify(emitter, interested ? times(1) : never()).emitAction(any(), any());
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2 })
    void otherEmittersShouldBeCalledWhenOneIsInterestedThrows(int flakyEmitterIndex) {
        // Given
        mockedProxyChannelStateMachine();

        var emitters = new AuditEmitter[]{
                mock(AuditEmitter.class),
                mock(AuditEmitter.class),
                mock(AuditEmitter.class)
        };
        for (int i = 0; i < emitters.length; i++) {
            if (i == flakyEmitterIndex) {
                doThrow(new RuntimeException("Ooops!")).when(emitters[i]).isInterested(any(), any());
            }
            else {
                doReturn(true).when(emitters[i]).isInterested(any(), any());
            }
        }

        var logger = new AuditLoggerImpl(List.of(emitters),
                () -> ClientActorImpl.of(pcsm.clientAddress(),
                        pcsm.sessionId(),
                        pcsm.authenticatedSubject()),
                Instant::now);

        // when
        logger.action("Foo")
                .withObjectRef(Map.of("vc", "my-virtual-cluster"))
                .addToContext("A", "a")
                .log();

        // then
        for (int i = 0; i < emitters.length; i++) {
            verify(emitters[i], i == flakyEmitterIndex ? never() : times(1)).emitAction(any(), any());
        }
    }

    @Test
    void allEmittersShouldBeCalledWhenOneEmitActionThrows() {
        // Given
        mockedProxyChannelStateMachine();

        var emitter1 = mock(AuditEmitter.class);
        doReturn(true).when(emitter1).isInterested(any(), any());
        var emitter2 = mock(AuditEmitter.class);
        doReturn(true).when(emitter2).isInterested(any(), any());
        doThrow(new RuntimeException("Ooops!")).when(emitter2).emitAction(any(), any());
        var emitter3 = mock(AuditEmitter.class);
        doReturn(true).when(emitter3).isInterested(any(), any());

        var logger = new AuditLoggerImpl(List.of(emitter1, emitter2, emitter3),
                () -> ClientActorImpl.of(pcsm.clientAddress(),
                        pcsm.sessionId(),
                        pcsm.authenticatedSubject()),
                Instant::now);

        // when
        logger.action("Foo")
                .withObjectRef(Map.of("vc", "my-virtual-cluster"))
                .addToContext("A", "a")
                .log();

        // then
        verify(emitter1).emitAction(any(), any());
        verify(emitter2).emitAction(any(), any());
        verify(emitter3).emitAction(any(), any());
    }

    static List<Arguments> shouldRenderActionAsJson() {
        return List.of(
                Arguments.argumentSet("successful", (Function<AuditLogger, AuditableActionBuilder>) (AuditLogger logger) -> logger.action("Foo"),
                        """
                                {
                                  "time" : "1970-01-01T00:00:00Z",
                                  "action" : "Foo",
                                  "status" : null,
                                  "reason" : null,
                                  "actor" : {
                                    "type" : "Client",
                                    "srcAddr" : "127.0.0.1/<unresolved>:12345",
                                    "session" : "ABC-1234",
                                    "principals" : [ {
                                      "type" : "io.kroxylicious.proxy.authentication.User",
                                      "name" : "alice"
                                    } ]
                                  },
                                  "objectRef" : {
                                    "vc" : "my-virtual-cluster"
                                  },
                                  "context" : {
                                    "bool" : true,
                                    "bools" : [ true, false ],
                                    "double" : 3.5,
                                    "doubles" : [ 3.5, -1.0E-10 ],
                                    "int" : 1,
                                    "ints" : [ 1, 42 ],
                                    "str" : "a",
                                    "strs" : [ "a", "b" ]
                                  }
                                }"""),
                Arguments.argumentSet("unsuccessful",
                        (Function<AuditLogger, AuditableActionBuilder>) (AuditLogger logger) -> logger.actionWithOutcome("Foo", "Bad", "Oh dear!"),
                        """
                                {
                                  "time" : "1970-01-01T00:00:00Z",
                                  "action" : "Foo",
                                  "status" : "Bad",
                                  "reason" : "Oh dear!",
                                  "actor" : {
                                    "type" : "Client",
                                    "srcAddr" : "127.0.0.1/<unresolved>:12345",
                                    "session" : "ABC-1234",
                                    "principals" : [ {
                                      "type" : "io.kroxylicious.proxy.authentication.User",
                                      "name" : "alice"
                                    } ]
                                  },
                                  "objectRef" : {
                                    "vc" : "my-virtual-cluster"
                                  },
                                  "context" : {
                                    "bool" : true,
                                    "bools" : [ true, false ],
                                    "double" : 3.5,
                                    "doubles" : [ 3.5, -1.0E-10 ],
                                    "int" : 1,
                                    "ints" : [ 1, 42 ],
                                    "str" : "a",
                                    "strs" : [ "a", "b" ]
                                  }
                                }"""));
    }

    @ParameterizedTest
    @MethodSource
    void shouldRenderActionAsJson(Function<AuditLogger, AuditableActionBuilder> fn,
                                  String expectedJson)
            throws JsonProcessingException {
        // Given
        mockedProxyChannelStateMachine();

        String[] s = new String[]{ null };
        var logger = new AuditLoggerImpl(List.of(new AuditEmitter() {
            @Override
            public boolean isInterested(String action, @Nullable String status) {
                return true;
            }

            @Override
            public void emitAction(AuditableAction action, Context context) {
                s[0] = context.asString(action, TextFormat.KROXYLICIOUS_JSON_V1);
            }

            @Override
            public void close() {

            }
        }),
                () -> ClientActorImpl.of(pcsm.clientAddress(),
                        pcsm.sessionId(),
                        pcsm.authenticatedSubject()),
                () -> Instant.EPOCH);

        // when
        fn.apply(logger)
                .withObjectRef(Map.of("vc", "my-virtual-cluster"))
                .addToContext("bool", true)
                .addToContext("int", 1L)
                .addToContext("double", 3.5)
                .addToContext("str", "a")
                .addToContext("strs", new String[]{ "a", "b" })
                .addToContext("bools", new boolean[]{ true, false })
                .addToContext("ints", new long[]{ 1L, 42L })
                .addToContext("doubles", new double[]{ 3.5, -1e-10 })
                .log();

        // then
        assertThat(s[0]).isNotNull();
        assertThatCode(() -> objectMapper.readTree(s[0]))
                .as("Should be valid JSON")
                .doesNotThrowAnyException();
        assertThat(objectMapper.writeValueAsString(objectMapper.readTree(s[0]))).isEqualTo(expectedJson);

    }

}