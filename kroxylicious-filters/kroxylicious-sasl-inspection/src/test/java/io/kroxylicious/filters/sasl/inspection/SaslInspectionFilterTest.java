/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;

import javax.security.sasl.SaslException;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.authentication.SaslSubjectBuilder;
import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.SubjectBuildingException;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.proxy.filter.ResponseFilterResult;
import io.kroxylicious.proxy.filter.ResponseFilterResultBuilder;
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SaslInspectionFilterTest {

    @Mock(strictness = LENIENT)
    private FilterContext context;

    @Mock(strictness = LENIENT)
    private SaslSubjectBuilder subjectBuilder;

    @Captor
    private ArgumentCaptor<ApiMessage> apiMessageCaptor;

    @Captor
    private ArgumentCaptor<ResponseHeaderData> responseHeaderDataCaptor;

    @BeforeEach
    void setUp() {
        when(context.forwardRequest(any(RequestHeaderData.class), apiMessageCaptor.capture())).then(invocationOnMock -> {
            var filterResult = mock(RequestFilterResult.class);
            lenient().when(filterResult.message()).thenReturn(apiMessageCaptor.getValue());
            return CompletableFuture.completedFuture(filterResult);
        });

        when(context.forwardResponse(any(ResponseHeaderData.class), apiMessageCaptor.capture())).then(invocationOnMock -> {
            var filterResult = mock(ResponseFilterResult.class);
            lenient().when(filterResult.message()).thenReturn(apiMessageCaptor.getValue());
            return CompletableFuture.completedFuture(filterResult);
        });

        when(context.requestFilterResultBuilder()).then(invocationOnMock -> {
            var builder = mock(RequestFilterResultBuilder.class);
            var filterResult = mock(RequestFilterResult.class);

            var closeOrTerminalStage = mock(CloseOrTerminalStage.class);
            lenient().when(closeOrTerminalStage.completed()).thenReturn(CompletableFuture.completedStage(filterResult));
            lenient().when(closeOrTerminalStage.build()).thenReturn(filterResult);

            when(builder.shortCircuitResponse(apiMessageCaptor.capture())).then(invocation -> {
                lenient().when(filterResult.shortCircuitResponse()).thenReturn(true);
                lenient().when(filterResult.message()).thenReturn(apiMessageCaptor.getValue());
                lenient().when(closeOrTerminalStage.withCloseConnection()).then(closeInvocation -> {
                    lenient().when(filterResult.closeConnection()).thenReturn(true);
                    return closeOrTerminalStage;
                });
                return closeOrTerminalStage;
            });
            return builder;
        });

        when(context.responseFilterResultBuilder()).then(invocationOnMock -> {
            var builder = mock(ResponseFilterResultBuilder.class);
            var filterResult = mock(ResponseFilterResult.class);

            var closeOrTerminalStage = mock(CloseOrTerminalStage.class);
            lenient().when(closeOrTerminalStage.completed()).thenReturn(CompletableFuture.completedStage(filterResult));
            lenient().when(closeOrTerminalStage.build()).thenReturn(filterResult);
            lenient().when(closeOrTerminalStage.withCloseConnection()).then(invocation -> {
                lenient().when(filterResult.closeConnection()).thenReturn(true);
                return closeOrTerminalStage;
            });

            when(builder.forward(responseHeaderDataCaptor.capture(), apiMessageCaptor.capture())).then(invocation -> {
                lenient().when(filterResult.message()).thenReturn(apiMessageCaptor.getValue());
                return closeOrTerminalStage;
            });
            return builder;
        });
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void shouldRejectNullConfig() {
        assertThatThrownBy(() -> new SaslInspectionFilter(null, subjectBuilder))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldForwardHandshakeUpstream() {
        // Given
        var filter = new SaslInspectionFilter(Map.of("PLAIN", new PlainSaslObserverFactory()), subjectBuilder);

        var downstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism("PLAIN");
        var downstreamHandshakeRequestHeader = new RequestHeaderData().setRequestApiKey(downstreamHandshakeRequest.apiKey())
                .setRequestApiVersion(downstreamHandshakeRequest.highestSupportedVersion());
        var expectedUpstreamHandshakeRequest = downstreamHandshakeRequest.duplicate();

        // When
        var actualUpstreamHandshakeRequest = filter.onRequest(
                ApiKeys.forId(downstreamHandshakeRequest.apiKey()),
                downstreamHandshakeRequestHeader,
                downstreamHandshakeRequest, context);

        // Then
        assertThat(actualUpstreamHandshakeRequest)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedUpstreamHandshakeRequest));
    }

    /**
     * The <a href="https://kafka.apache.org/protocol#sasl_handshake">protocol spec says</a>:
     * <blockquote>
     * If the mechanism is enabled in the server, the server sends a successful response and continues with SASL authentication.
     * </blockquote>
     */
    @Test
    void shouldReturnHandshakeResponseDownstreamWhenMechanismsAgree() {
        // Given
        var filter = new SaslInspectionFilter(Map.of("PLAIN", new PlainSaslObserverFactory()), subjectBuilder);

        var downstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism("PLAIN");
        var downstreamHandshakeRequestHeader = new RequestHeaderData().setRequestApiKey(downstreamHandshakeRequest.apiKey())
                .setRequestApiVersion(downstreamHandshakeRequest.highestSupportedVersion());
        var expectedUpstreamHandshakeRequest = downstreamHandshakeRequest.duplicate();

        var actualUpstreamHandshakeRequest = filter.onRequest(
                ApiKeys.forId(downstreamHandshakeRequest.apiKey()),
                downstreamHandshakeRequestHeader,
                downstreamHandshakeRequest, context);

        assertThat(actualUpstreamHandshakeRequest)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedUpstreamHandshakeRequest));

        var upstreamHandshakeResponse = new SaslHandshakeResponseData().setMechanisms(List.of("PLAIN"));
        var upstreamHandshakeResponseHeader = new ResponseHeaderData();
        var expectedDownstreamHandshakeResponse = upstreamHandshakeResponse.duplicate();

        var actualDownstreamHandshakeResponse = filter.onSaslHandshakeResponse(upstreamHandshakeResponse.highestSupportedVersion(), upstreamHandshakeResponseHeader,
                upstreamHandshakeResponse, context);

        assertThat(actualDownstreamHandshakeResponse)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedDownstreamHandshakeResponse));

    }

    /**
     * The <a href="https://kafka.apache.org/protocol#sasl_handshake">protocol spec says</a>:
     * <blockquote>
     * If the requested mechanism is not enabled in the server, the server responds with the list of supported mechanisms and closes the client connection.
     * </blockquote>
     */
    @Test
    void shouldReturnHandshakeErrorResponseDownstreamWhenClientMechanismUnknownToProxy() {
        // Given
        var filter = new SaslInspectionFilter(Map.of("PLAIN", new PlainSaslObserverFactory()), subjectBuilder);

        var downstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism("NOTAMECH");
        var downstreamHandshakeRequestHeader = new RequestHeaderData().setRequestApiKey(downstreamHandshakeRequest.apiKey())
                .setRequestApiVersion(downstreamHandshakeRequest.highestSupportedVersion());
        var expectedUpstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism(SaslInspectionFilter.PROBE_UPSTREAM);

        var actualUpstreamHandshakeRequest = filter.onRequest(
                ApiKeys.forId(downstreamHandshakeRequest.apiKey()),
                downstreamHandshakeRequestHeader,
                downstreamHandshakeRequest, context);
        assertThat(actualUpstreamHandshakeRequest)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedUpstreamHandshakeRequest));

        var upstreamHandshakeResponse = new SaslHandshakeResponseData().setMechanisms(List.of("PLAIN", "SCRAM-SHA-256"))
                .setErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM.code());
        var upstreamHandshakeResponseHeader = new ResponseHeaderData();
        var expectedDownstreamHandshakeResponse = new SaslHandshakeResponseData().setMechanisms(List.of("PLAIN")).setErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM.code());

        // When
        var forwardedHandshakeResponse = filter.onSaslHandshakeResponse(upstreamHandshakeResponse.highestSupportedVersion(), upstreamHandshakeResponseHeader,
                upstreamHandshakeResponse, context);

        // Then
        assertThat(forwardedHandshakeResponse)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> {
                    assertThat(rfr.message())
                            .isEqualTo(expectedDownstreamHandshakeResponse);
                    assertThat(rfr.closeConnection())
                            .isTrue();
                });
    }

    @Test
    void shouldReturnHandshakeErrorResponseDownstreamWhenClientMechanismUnknownToBroker() {
        // Given
        var filter = new SaslInspectionFilter(Map.of("PLAIN", new PlainSaslObserverFactory(), "SCRAM-SHA-256", new ScramSha256SaslObserverFactory()), subjectBuilder);

        var downstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism("PLAIN");
        var downstreamHandshakeRequestHeader = new RequestHeaderData().setRequestApiKey(downstreamHandshakeRequest.apiKey())
                .setRequestApiVersion(downstreamHandshakeRequest.highestSupportedVersion());
        var expectedUpstreamHandshakeRequest = downstreamHandshakeRequest.duplicate();

        var actualUpstreamHandshakeRequest = filter.onRequest(
                ApiKeys.forId(downstreamHandshakeRequest.apiKey()),
                downstreamHandshakeRequestHeader,
                downstreamHandshakeRequest, context);
        assertThat(actualUpstreamHandshakeRequest)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedUpstreamHandshakeRequest));

        var upstreamHandshakeResponse = new SaslHandshakeResponseData().setMechanisms(List.of("SCRAM-SHA-256")).setErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM.code());
        var upstreamHandshakeResponseHeader = new ResponseHeaderData();
        var expectedDownstreamHandshakeResponse = new SaslHandshakeResponseData().setMechanisms(List.of("SCRAM-SHA-256"))
                .setErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM.code());

        // When
        var forwardedHandshakeResponse = filter.onSaslHandshakeResponse(upstreamHandshakeResponse.highestSupportedVersion(), upstreamHandshakeResponseHeader,
                upstreamHandshakeResponse, context);

        // Then
        assertThat(forwardedHandshakeResponse)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> {
                    assertThat(rfr.message())
                            .isEqualTo(expectedDownstreamHandshakeResponse);
                    assertThat(rfr.closeConnection())
                            .isTrue();
                });
    }

    @Test
    void shouldDetectMissingHandshake() {
        // Given
        var filter = new SaslInspectionFilter(Map.of("PLAIN", new PlainSaslObserverFactory()), subjectBuilder);

        // Omits handshake

        var downstreamAuthenticateRequest = new SaslAuthenticateRequestData().setAuthBytes(TestData.SASL_PLAIN_CLIENT_INITIAL);
        var downstreamAuthenticateRequestHeader = new RequestHeaderData().setRequestApiKey(downstreamAuthenticateRequest.apiKey())
                .setRequestApiVersion(downstreamAuthenticateRequest.highestSupportedVersion());
        var expectedDownstreamAuthenticateShortCircuitResponse = new SaslAuthenticateResponseData()
                .setErrorCode(Errors.ILLEGAL_SASL_STATE.code())
                .setErrorMessage("SaslHandshake has not been performed");

        // When
        var actualUpstreamAuthenticateRequest = filter.onSaslAuthenticateRequest(downstreamAuthenticateRequest.highestSupportedVersion(),
                downstreamAuthenticateRequestHeader, downstreamAuthenticateRequest, context);

        // Then
        assertThat(actualUpstreamAuthenticateRequest)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedDownstreamAuthenticateShortCircuitResponse));

        verify(context, never()).clientSaslAuthenticationSuccess(anyString(), anyString());
    }

    @Test
    void shouldDetectUnexpectedSecondHandshake() {
        // Given
        var filter = new SaslInspectionFilter(Map.of("PLAIN", new PlainSaslObserverFactory()), subjectBuilder);

        var downstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism("PLAIN");
        var downstreamHandshakeRequestHeader = new RequestHeaderData().setRequestApiKey(downstreamHandshakeRequest.apiKey())
                .setRequestApiVersion(downstreamHandshakeRequest.highestSupportedVersion());
        var expectedUpstreamHandshakeRequest = downstreamHandshakeRequest.duplicate();

        var actualUpstreamHandshakeRequest = filter.onRequest(
                ApiKeys.forId(downstreamHandshakeRequest.apiKey()),
                downstreamHandshakeRequestHeader,
                downstreamHandshakeRequest, context);

        assertThat(actualUpstreamHandshakeRequest)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedUpstreamHandshakeRequest));

        var unexpectedSecondDownstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism("PLAIN");
        var expectedResponse = new SaslHandshakeResponseData().setErrorCode(Errors.ILLEGAL_SASL_STATE.code());

        // When
        var response = filter.onRequest(
                ApiKeys.forId(unexpectedSecondDownstreamHandshakeRequest.apiKey()),
                downstreamHandshakeRequestHeader,
                unexpectedSecondDownstreamHandshakeRequest, context);

        // Then
        assertThat(response)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> {
                    assertThat(rfr.message())
                            .isEqualTo(expectedResponse);
                    assertThat(rfr.closeConnection()).isTrue();
                });

        verify(context, never()).clientSaslAuthenticationSuccess(anyString(), anyString());
    }

    @Test
    void shouldDetectMalformedClientInitialResponse() {
        // Given
        var filter = new SaslInspectionFilter(Map.of("PLAIN", new PlainSaslObserverFactory()), subjectBuilder);

        doSaslHandshakeRequest("PLAIN", filter);
        doSaslHandshakeResponse("PLAIN", filter);

        var badClientFirst = "\0too\0many\0\tokens\0for\0SASL\0PLAIN".getBytes(StandardCharsets.US_ASCII);
        var authenticateRequest = new SaslAuthenticateRequestData().setAuthBytes(badClientFirst);
        var authenticateRequestHeader = new RequestHeaderData().setRequestApiKey(authenticateRequest.apiKey())
                .setRequestApiVersion(SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION);
        var expectedAuthenticateResponse = new SaslAuthenticateResponseData().setErrorCode(Errors.ILLEGAL_SASL_STATE.code())
                .setErrorMessage("Proxy cannot extract authorizationId from SASL authenticate request");

        // When
        var actualAuthenticateResponse = filter.onSaslAuthenticateRequest(SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION,
                authenticateRequestHeader, authenticateRequest, context);

        // Then
        assertThat(actualAuthenticateResponse)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> {
                    assertThat(rfr.message())
                            .isEqualTo(expectedAuthenticateResponse);
                    assertThat(rfr.closeConnection()).isTrue();
                });
    }

    @Test
    void shouldDetectMissingAuthzId() throws SaslException {
        // Given

        // This SASL observer fails to collect an authz
        var saslObserver = mock(SaslObserver.class);
        when(saslObserver.authorizationId()).thenThrow(new SaslException("mock - no authz"));
        when(saslObserver.clientResponse(any(byte[].class))).thenReturn(true);
        when(saslObserver.isFinished()).thenReturn(true);

        var saslObserverFactory = mock(SaslObserverFactory.class);
        when(saslObserverFactory.createObserver()).thenReturn(saslObserver);

        var filter = new SaslInspectionFilter(Map.of("PLAIN", saslObserverFactory), subjectBuilder);

        doSaslHandshakeRequest("PLAIN", filter);
        doSaslHandshakeResponse("PLAIN", filter);

        doSaslAuthenticateRequest("anything".getBytes(StandardCharsets.UTF_8), filter);

        var authenticateResponse = new SaslAuthenticateResponseData();
        var expectedAuthenticateResponse = new SaslAuthenticateResponseData().setErrorCode(Errors.ILLEGAL_SASL_STATE.code());

        // When
        var actualResponse = filter.onSaslAuthenticateResponse(authenticateResponse.highestSupportedVersion(),
                new ResponseHeaderData(),
                authenticateResponse, context);

        // Then
        assertThat(actualResponse)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> {
                    assertThat(rfr.message())
                            .isEqualTo(expectedAuthenticateResponse);
                    assertThat(rfr.closeConnection()).isTrue();
                });
    }

    /**
     * The <a href="https://kafka.apache.org/protocol#sasl_handshake">protocol spec says</a>:
     * <blockquote>
     * The error code in the final message from the broker will indicate if authentication succeeded or failed.
     * ... Otherwise, the client connection is closed.
     * </blockquote>
     */
    @Test
    void shouldReturnAuthenticationErrorResponseDownstreamWhenBrokerSignalsAuthenticationError() {
        // Given
        var filter = new SaslInspectionFilter(Map.of("PLAIN", new PlainSaslObserverFactory()), subjectBuilder);

        // When
        doSaslHandshakeRequest("PLAIN", filter);
        doSaslHandshakeResponse("PLAIN", filter);

        doSaslAuthenticateRequest(TestData.SASL_PLAIN_CLIENT_INITIAL, filter);

        var upstreamAuthenticateResponse = new SaslAuthenticateResponseData().setErrorCode(Errors.SASL_AUTHENTICATION_FAILED.code());
        var upstreamAuthenticateResponseHeader = new ResponseHeaderData();
        var expectedDownstreamAuthenticateResponse = upstreamAuthenticateResponse.duplicate();

        var actualDownstreamAuthenticateResponse = filter.onSaslAuthenticateResponse(upstreamAuthenticateResponse.highestSupportedVersion(),
                upstreamAuthenticateResponseHeader,
                upstreamAuthenticateResponse, context);

        // Then
        assertThat(actualDownstreamAuthenticateResponse)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> {
                    assertThat(rfr.message())
                            .isEqualTo(expectedDownstreamAuthenticateResponse);
                    assertThat(rfr.closeConnection()).isTrue();
                });

        verify(context, never()).clientSaslAuthenticationSuccess(anyString(), anyString());
        verify(context).clientSaslAuthenticationFailure(eq("PLAIN"), eq("tim"), isA(SaslException.class));
    }

    static Stream<Arguments> successfulSaslAuthentications() {
        return Stream.of(
                Arguments.argumentSet("SASL PLAIN (only authcid provided)",
                        new PlainSaslObserverFactory(),
                        new InitialResponse(TestData.SASL_PLAIN_CLIENT_INITIAL),
                        List.of(new ChallengeResponse(new byte[0], null)),
                        "tim"),
                Arguments.argumentSet("SASL PLAIN (authzid used in preference to authcid)",
                        new PlainSaslObserverFactory(),
                        new InitialResponse(TestData.SASL_PLAIN_CLIENT_INITIAL_WITH_AUTHZID),
                        List.of(new ChallengeResponse(new byte[0], null)),
                        "Ursel"),
                Arguments.argumentSet("SASL SCRAM-SHA-256",
                        new ScramSha256SaslObserverFactory(),
                        new InitialResponse(TestData.SASL_SCRAM_SHA_256_CLIENT_INITIAL),
                        List.of(new ChallengeResponse(TestData.SASL_SCRAM_SHA_256_SERVER_FIRST, TestData.SASL_SCRAM_SHA_256_CLIENT_FINAL),
                                new ChallengeResponse(TestData.SASL_SCRAM_SHA_256_SERVER_FINAL, null)),
                        "user"),
                Arguments.argumentSet("SASL SCRAM-SHA-512",
                        new ScramSha512SaslObserverFactory(),
                        new InitialResponse(TestData.SASL_SCRAM_SHA_512_CLIENT_INITIAL),
                        List.of(new ChallengeResponse(
                                TestData.SASL_SCRAM_SHA_512_SERVER_FIRST,
                                TestData.SASL_SCRAM_SHA_512_CLIENT_FINAL),
                                new ChallengeResponse(
                                        TestData.SASL_SCRAM_SHA_512_SERVER_FINAL,
                                        null)),
                        "user"),
                Arguments.argumentSet("SASL OAUTHBEARER",
                        new OauthBearerSaslObserverFactory(),
                        new InitialResponse(TestData.SASL_OAUTHBEARER_SIGNED_JWT),
                        List.of(new ChallengeResponse(new byte[0], null)),
                        "johndoe"));
    }

    @ParameterizedTest
    @MethodSource("successfulSaslAuthentications")
    void shouldAuthenticateSuccessfully(SaslObserverFactory observerFactory, InitialResponse initialResponse, List<ChallengeResponse> challengeResponses,
                                        String expectedAuthorizedId) {
        // Given
        when(subjectBuilder.buildSaslSubject(any())).then(a -> {
            var ctx = (SaslSubjectBuilder.Context) a.getArguments()[0];
            return CompletableFuture.completedFuture(new Subject(new User(ctx.clientSaslContext().authorizationId())));
        });

        // When
        doAuthenticateSuccessfully(observerFactory, initialResponse, challengeResponses);

        // Then
        verify(context).clientSaslAuthenticationSuccess(observerFactory.mechanismName(), new Subject(new User(expectedAuthorizedId)));
        verify(context, never()).clientSaslAuthenticationFailure(anyString(), anyString(), nullable(Exception.class));

    }

    static Stream<Arguments> successfulSaslReauthentications() {
        return Stream.of(
                Arguments.argumentSet("reauth",
                        new PlainSaslObserverFactory(),
                        new InitialResponse(TestData.SASL_PLAIN_CLIENT_INITIAL),
                        List.of(new ChallengeResponse(new byte[0], null)),
                        new InitialResponse(TestData.SASL_PLAIN_CLIENT_INITIAL),
                        List.of(new ChallengeResponse(new byte[0], null)),
                        (Consumer<FilterContext>) context -> {
                            verify(context, never()).clientSaslAuthenticationFailure(anyString(), anyString(), nullable(Exception.class));
                            verify(context, times(2)).clientSaslAuthenticationSuccess("PLAIN", new Subject(new User("tim")));
                        }),
                Arguments.argumentSet("reauth changes of authzid",
                        new PlainSaslObserverFactory(),
                        new InitialResponse(TestData.SASL_PLAIN_CLIENT_INITIAL),
                        List.of(new ChallengeResponse(new byte[0], null)),
                        new InitialResponse("\0timmy\0tanstaaftanstaaf".getBytes(StandardCharsets.US_ASCII)),
                        List.of(new ChallengeResponse(new byte[0], null)),
                        (Consumer<FilterContext>) context -> {
                            verify(context, never()).clientSaslAuthenticationFailure(anyString(), anyString(), nullable(Exception.class));
                            verify(context).clientSaslAuthenticationSuccess("PLAIN", new Subject(new User("tim")));
                            verify(context).clientSaslAuthenticationSuccess("PLAIN", new Subject(new User("timmy")));
                        }));
    }

    @ParameterizedTest
    @MethodSource("successfulSaslReauthentications")
    void shouldReauthenticateSuccessfully(SaslObserverFactory observerFactory, InitialResponse initialResponse, List<ChallengeResponse> challengeResponses,
                                          InitialResponse reauthInitialResponse, List<ChallengeResponse> reauthChallengeResponses, Consumer<FilterContext> verify) {
        // Given
        when(subjectBuilder.buildSaslSubject(any())).then(a -> {
            var ctx = (SaslSubjectBuilder.Context) a.getArguments()[0];
            return CompletableFuture.completedFuture(new Subject(new User(ctx.clientSaslContext().authorizationId())));
        });

        // When
        doAuthenticateSuccessfully(observerFactory, initialResponse, challengeResponses);
        doAuthenticateSuccessfully(observerFactory, reauthInitialResponse, reauthChallengeResponses);

        // Then
        verify.accept(context);
    }

    @Test
    void shouldDetectUnexpectedReauthentication() {
        // Given
        when(subjectBuilder.buildSaslSubject(any())).then(a -> {
            var ctx = (SaslSubjectBuilder.Context) a.getArguments()[0];
            return CompletableFuture.completedFuture(new Subject(new User(ctx.clientSaslContext().authorizationId())));
        });

        // When
        var filter = new SaslInspectionFilter(Map.of("PLAIN", new PlainSaslObserverFactory()), subjectBuilder);

        doSaslHandshakeRequest("PLAIN", filter);
        doSaslHandshakeResponse("PLAIN", filter);

        doSaslAuthenticateRequest(TestData.SASL_PLAIN_CLIENT_INITIAL, filter, (short) 0);
        doSaslAuthenticateResponse(new byte[0], filter, 1);

        var unexpectedHandshake = new SaslHandshakeRequestData().setMechanism("PLAIN");
        var unexpectedHandshakeHeader = new RequestHeaderData().setRequestApiKey(unexpectedHandshake.apiKey());
        var expectedResponse = new SaslHandshakeResponseData().setErrorCode(Errors.ILLEGAL_SASL_STATE.code());

        // When

        var actualUpstreamHandshakeRequest = filter.onRequest(
                ApiKeys.forId(unexpectedHandshake.apiKey()),
                unexpectedHandshakeHeader,
                unexpectedHandshake, context);

        // Then
        assertThat(actualUpstreamHandshakeRequest)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> {
                    assertThat(rfr.message())
                            .isEqualTo(expectedResponse);
                    assertThat(rfr.closeConnection()).isTrue();
                });
    }

    @Test
    void shouldReportRecentlyExpiredTokenAsFailedAuthentication() {
        // Given
        var filter = new SaslInspectionFilter(Map.of("OAUTHBEAER", new OauthBearerSaslObserverFactory()), subjectBuilder);

        doSaslHandshakeRequest("OAUTHBEAER", filter);
        doSaslHandshakeResponse("OAUTHBEAER", filter);

        doSaslAuthenticateRequest(TestData.SASL_OAUTHBEARER_SIGNED_JWT, filter, (short) 0);
        doSaslAuthenticateResponse(new byte[0], filter, 0 /* a session lifespan of 0ms means the client must re-auth next */);

        verify(context, never()).clientSaslAuthenticationSuccess(anyString(), anyString());
        verify(context).clientSaslAuthenticationFailure(eq("OAUTHBEARER"), eq("johndoe"), isA(SaslException.class));
    }

    static Stream<Arguments> shouldHandleSaslBuilderException() {
        return successfulSaslAuthentications()
                .filter(s -> !((SaslObserverFactory) s.get()[0]).mechanismName().startsWith("SCRAM"));
    }

    @ParameterizedTest
    @MethodSource
    void shouldHandleSaslBuilderException(SaslObserverFactory observerFactory,
                                          InitialResponse initialResponse, List<ChallengeResponse> challengeResponses,
                                          String expectedAuthorizedId) {
        // Given
        RuntimeException oops = new RuntimeException("Oops");
        when(subjectBuilder.buildSaslSubject(any())).then(a -> {
            return CompletableFuture.failedStage(oops);
        });
        var filter = new SaslInspectionFilter(Map.of(observerFactory.mechanismName(), observerFactory), subjectBuilder);

        // When
        doSaslHandshakeRequest(observerFactory.mechanismName(), filter);
        doSaslHandshakeResponse(observerFactory.mechanismName(), filter);
        doSaslAuthenticateRequest(initialResponse.response(), filter);

        var upstreamAuthenticateResponseHeader = new ResponseHeaderData();
        var upstreamAuthenticateResponse = new SaslAuthenticateResponseData().setSessionLifetimeMs(1);
        var expectedDownstreamAuthenticateResponse = new SaslAuthenticateResponseData().setErrorCode(Errors.ILLEGAL_SASL_STATE.code()).setSessionLifetimeMs(1);

        var actualDownstreamAuthenticateResponse = filter.onSaslAuthenticateResponse(upstreamAuthenticateResponse.highestSupportedVersion(),
                upstreamAuthenticateResponseHeader,
                upstreamAuthenticateResponse, context);

        // Then
        assertThat(actualDownstreamAuthenticateResponse)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> {
                    assertThat(rfr.message())
                            .isEqualTo(expectedDownstreamAuthenticateResponse);
                    assertThat(rfr.closeConnection()).isTrue();
                });

        // Then
        verify(context).clientSaslAuthenticationFailure(eq(observerFactory.mechanismName()), eq(expectedAuthorizedId), isA(SubjectBuildingException.class));
        verify(context, never()).clientSaslAuthenticationSuccess(anyString(), anyString());
        verify(context, never()).clientSaslAuthenticationSuccess(anyString(), any(Subject.class));
        // verify(context).r

    }

    private void doAuthenticateSuccessfully(SaslObserverFactory saslObserverFactory, InitialResponse initialResponse, List<ChallengeResponse> challengeResponses) {
        // Given
        var filter = new SaslInspectionFilter(Map.of(saslObserverFactory.mechanismName(), saslObserverFactory), subjectBuilder);

        // When
        doSaslHandshakeRequest(saslObserverFactory.mechanismName(), filter);
        doSaslHandshakeResponse(saslObserverFactory.mechanismName(), filter);

        doSaslAuthenticateRequest(initialResponse.response(), filter);

        challengeResponses.forEach(cr -> {
            doSaslAuthenticateResponse(cr.challenge(), filter, 1);
            Optional.ofNullable(cr.response()).ifPresent(r -> doSaslAuthenticateRequest(r, filter));
        });

    }

    private void doSaslHandshakeRequest(String mechanism, SaslInspectionFilter filter) {
        doSaslHandshakeRequest(mechanism, filter, SaslHandshakeRequestData.HIGHEST_SUPPORTED_VERSION);
    }

    private void doSaslHandshakeRequest(String mechanism, SaslInspectionFilter filter, short version) {

        var handshakeRequest = new SaslHandshakeRequestData().setMechanism(mechanism);
        var handshakeRequestHeader = new RequestHeaderData().setRequestApiKey(handshakeRequest.apiKey())
                .setRequestApiVersion(version);
        var expectedHandshakeRequest = handshakeRequest.duplicate();

        var actualHandshakeRequest = filter.onRequest(
                ApiKeys.forId(handshakeRequest.apiKey()),
                handshakeRequestHeader,
                handshakeRequest, context);

        assertThat(actualHandshakeRequest)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedHandshakeRequest));

    }

    private void doSaslHandshakeResponse(String mechanism, SaslInspectionFilter filter) {
        var handshakeResponse = new SaslHandshakeResponseData().setMechanisms(List.of(mechanism));
        var expectedHandshakeResponse = handshakeResponse.duplicate();

        var actualHandshakeResponse = filter.onSaslHandshakeResponse(handshakeResponse.highestSupportedVersion(), new ResponseHeaderData(),
                handshakeResponse, context);

        assertThat(actualHandshakeResponse)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedHandshakeResponse));
    }

    private void doSaslAuthenticateRequest(byte[] response, SaslInspectionFilter filter) {
        doSaslAuthenticateRequest(response, filter, SaslAuthenticateRequestData.HIGHEST_SUPPORTED_VERSION);
    }

    private void doSaslAuthenticateRequest(byte[] response, SaslInspectionFilter filter, short version) {
        var authenticateRequest = new SaslAuthenticateRequestData().setAuthBytes(response);
        var authenticateRequestHeader = new RequestHeaderData().setRequestApiKey(authenticateRequest.apiKey())
                .setRequestApiVersion(version);
        var expectedAuthenticateRequest = authenticateRequest.duplicate();

        var actualAuthenticateRequest = filter.onSaslAuthenticateRequest(version,
                authenticateRequestHeader, authenticateRequest, context);

        assertThat(actualAuthenticateRequest)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedAuthenticateRequest));
    }

    private void doSaslAuthenticateResponse(byte[] challenge, SaslInspectionFilter filter, int sessionLifetimeMs) {
        var authenticateResponse = new SaslAuthenticateResponseData().setAuthBytes(challenge).setSessionLifetimeMs(sessionLifetimeMs);
        doSaslAuthenticateResponse(filter, authenticateResponse, authenticateResponse.duplicate());
    }

    private void doSaslAuthenticateResponse(SaslInspectionFilter filter,
                                            SaslAuthenticateResponseData authenticateResponse,
                                            SaslAuthenticateResponseData expectedAuthenticateResponse) {
        var actualResponse = filter.onSaslAuthenticateResponse(authenticateResponse.highestSupportedVersion(),
                new ResponseHeaderData(),
                authenticateResponse, context);

        assertThat(actualResponse)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedAuthenticateResponse));
    }

    private interface SaslInteraction {
        byte[] response();
    }

    private record InitialResponse(byte[] response) implements SaslInteraction {}

    private record ChallengeResponse(byte[] challenge, @Nullable byte[] response) implements SaslInteraction {}
}
