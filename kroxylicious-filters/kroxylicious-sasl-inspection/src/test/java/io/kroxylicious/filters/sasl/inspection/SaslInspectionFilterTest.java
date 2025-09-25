/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
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

    // SASL PLAIN - Known good from https://datatracker.ietf.org/doc/html/rfc4616
    static final byte[] SASL_PLAIN_CLIENT_INITIAL = "\0tim\0tanstaaftanstaaf".getBytes(StandardCharsets.US_ASCII);
    static final byte[] SASL_PLAIN_CLIENT_INITIAL_WITH_AUTHZID = "Ursel\0Kurt\0xipj3plmq".getBytes(StandardCharsets.US_ASCII);

    // Known good from https://datatracker.ietf.org/doc/html/rfc7677
    static final byte[] SASL_SCRAM_SHA_256_CLIENT_INITIAL = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO".getBytes(StandardCharsets.US_ASCII);
    static final byte[] SASL_SCRAM_SHA_256_SERVER_FIRST = "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096".getBytes(
            StandardCharsets.US_ASCII);
    static final byte[] SASL_SCRAM_SHA_256_CLIENT_FINAL = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
            .getBytes(StandardCharsets.US_ASCII);
    static final byte[] SASL_SCRAM_SHA_256_SERVER_FINAL = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=".getBytes(StandardCharsets.US_ASCII);
    static final byte[] SASL_SCRAM_SHA_512_CLIENT_INITIAL = SASL_SCRAM_SHA_256_CLIENT_INITIAL;
    static final byte[] SASL_SCRAM_SHA_512_SERVER_FIRST = "r=rOprNGfwEbeRWgbNEkqO02431b08-2f89-4bad-a4e6-80c0564ec865,s=Yin2FuHTt/M0kJWb0t9OI32n2VmOGi3m+JfjOvuDF88=,i=4096"
            .getBytes(StandardCharsets.US_ASCII);
    static final byte[] SASL_SCRAM_SHA_512_CLIENT_FINAL = "c=biws,r=rOprNGfwEbeRWgbNEkqO02431b08-2f89-4bad-a4e6-80c0564ec865,p=Hc5yec3NmCD7t+kFRw4/3yD6/F3SQHc7AVYschRja+Bc3sbdjlA0eH1OjJc0DD4ghn1tnXN5/Wr6qm9xmaHt4A=="
            .getBytes(StandardCharsets.US_ASCII);
    static final byte[] SASL_SCRAM_SHA_512_SERVER_FINAL = "v=BQuhnKHqYDwQWS5jAw4sZed+C9KFUALsbrq81bB0mh+bcUUbbMPNNmBIupnS2AmyyDnG5CTBQtkjJ9kyY4kzmw==".getBytes(
            StandardCharsets.US_ASCII);
    static final byte[] SASL_SCRAM_SHA512_CLIENT_INITIAL_WITH_AUTHZID = "n,a=Ursel,n=user,r=rOprNGfwEbeRWgbNEkqO".getBytes(StandardCharsets.US_ASCII);

    @Mock(strictness = LENIENT)
    private FilterContext context;

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
    void rejectsNullConfig() {
        assertThatThrownBy(() -> new SaslInspectionFilter(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldForwardHandshakeUpstream() {
        // Given
        var filter = new SaslInspectionFilter(new Config(Set.of("PLAIN")));

        var downstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism("PLAIN");
        var downstreamHandshakeRequestHeader = new RequestHeaderData().setRequestApiKey(downstreamHandshakeRequest.apiKey())
                .setRequestApiVersion(downstreamHandshakeRequest.highestSupportedVersion());
        var expectedUpstreamHandshakeRequest = downstreamHandshakeRequest.duplicate();

        // When
        var actualUpstreamHandshakeRequest = filter.onSaslHandshakeRequest(downstreamHandshakeRequest.highestSupportedVersion(), downstreamHandshakeRequestHeader,
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
        var filter = new SaslInspectionFilter(new Config(Set.of("PLAIN")));

        var downstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism("PLAIN");
        var downstreamHandshakeRequestHeader = new RequestHeaderData().setRequestApiKey(downstreamHandshakeRequest.apiKey())
                .setRequestApiVersion(downstreamHandshakeRequest.highestSupportedVersion());
        var expectedUpstreamHandshakeRequest = downstreamHandshakeRequest.duplicate();

        var actualUpstreamHandshakeRequest = filter.onSaslHandshakeRequest(downstreamHandshakeRequest.highestSupportedVersion(), downstreamHandshakeRequestHeader,
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
        var filter = new SaslInspectionFilter(new Config(Set.of("PLAIN")));

        var downstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism("NOTAMECH");
        var downstreamHandshakeRequestHeader = new RequestHeaderData().setRequestApiKey(downstreamHandshakeRequest.apiKey())
                .setRequestApiVersion(downstreamHandshakeRequest.highestSupportedVersion());
        var expectedUpstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism(SaslInspectionFilter.PROBE_UPSTREAM);

        var actualUpstreamHandshakeRequest = filter.onSaslHandshakeRequest(downstreamHandshakeRequest.highestSupportedVersion(), downstreamHandshakeRequestHeader,
                downstreamHandshakeRequest, context);
        assertThat(actualUpstreamHandshakeRequest)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedUpstreamHandshakeRequest));

        var upstreamHandshakeResponse = new SaslHandshakeResponseData().setMechanisms(List.of("PLAIN", "SCRAM-SHA-256"));
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
        var filter = new SaslInspectionFilter(new Config(Set.of("PLAIN", "SCRAM-SHA-256")));

        var downstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism("PLAIN");
        var downstreamHandshakeRequestHeader = new RequestHeaderData().setRequestApiKey(downstreamHandshakeRequest.apiKey())
                .setRequestApiVersion(downstreamHandshakeRequest.highestSupportedVersion());
        var expectedUpstreamHandshakeRequest = downstreamHandshakeRequest.duplicate();

        var actualUpstreamHandshakeRequest = filter.onSaslHandshakeRequest(downstreamHandshakeRequest.highestSupportedVersion(), downstreamHandshakeRequestHeader,
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
        var filter = new SaslInspectionFilter(new Config(Set.of("PLAIN")));

        // Omits handshake

        var downstreamAuthenticateRequest = new SaslAuthenticateRequestData().setAuthBytes(SASL_PLAIN_CLIENT_INITIAL);
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
        var filter = new SaslInspectionFilter(new Config(Set.of("PLAIN")));

        var downstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism("PLAIN");
        var downstreamHandshakeRequestHeader = new RequestHeaderData().setRequestApiKey(downstreamHandshakeRequest.apiKey())
                .setRequestApiVersion(downstreamHandshakeRequest.highestSupportedVersion());
        var expectedUpstreamHandshakeRequest = downstreamHandshakeRequest.duplicate();

        var actualUpstreamHandshakeRequest = filter.onSaslHandshakeRequest(downstreamHandshakeRequest.highestSupportedVersion(), downstreamHandshakeRequestHeader,
                downstreamHandshakeRequest, context);

        assertThat(actualUpstreamHandshakeRequest)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedUpstreamHandshakeRequest));

        var unexpectedSecondDownstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism("PLAIN");
        var expectedResponse = new SaslHandshakeResponseData().setErrorCode(Errors.ILLEGAL_SASL_STATE.code());

        // When
        var response = filter.onSaslHandshakeRequest(unexpectedSecondDownstreamHandshakeRequest.highestSupportedVersion(), downstreamHandshakeRequestHeader,
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
        var filter = new SaslInspectionFilter(new Config(Set.of("PLAIN")));

        // When
        doSaslHandshakeRequest("PLAIN", filter);
        doSaslHandshakeResponse("PLAIN", filter);

        doSaslAuthenticateRequest(SASL_PLAIN_CLIENT_INITIAL, filter);

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
        verify(context).clientSaslAuthenticationFailure(eq("PLAIN"), eq("tim"), isA(SaslAuthenticationException.class));
    }

    static Stream<Arguments> successfulSaslAuthentications() {
        return Stream.of(
                Arguments.argumentSet("SASL PLAIN (only authcid provided)",
                        "PLAIN",
                        new InitialResponse(SASL_PLAIN_CLIENT_INITIAL),
                        List.of(new ChallengeResponse(new byte[0], null)),
                        "tim"),
                Arguments.argumentSet("SASL PLAIN (authzid and authcid provided)",
                        "PLAIN",
                        new InitialResponse(SASL_PLAIN_CLIENT_INITIAL_WITH_AUTHZID),
                        List.of(new ChallengeResponse(new byte[0], null)),
                        "Ursel"),
                Arguments.argumentSet("SASL SCRAM-SHA-256 (n (authcid) provided)",
                        "SCRAM-SHA-256",
                        new InitialResponse(SASL_SCRAM_SHA_256_CLIENT_INITIAL),
                        List.of(new ChallengeResponse(SASL_SCRAM_SHA_256_SERVER_FIRST, SASL_SCRAM_SHA_256_CLIENT_FINAL),
                                new ChallengeResponse(SASL_SCRAM_SHA_256_SERVER_FINAL, null)),
                        "user"),
                Arguments.argumentSet("SASL SCRAM-SHA-512 (a (authzid) and n (authcid) provided)",
                        "SCRAM-SHA-512",
                        new InitialResponse(SASL_SCRAM_SHA_512_CLIENT_INITIAL),
                        List.of(new ChallengeResponse(
                                SASL_SCRAM_SHA_512_SERVER_FIRST,
                                SASL_SCRAM_SHA_512_CLIENT_FINAL),
                                new ChallengeResponse(
                                        SASL_SCRAM_SHA_512_SERVER_FINAL,
                                        null)),
                        "user"),
                Arguments.argumentSet("SASL SCRAM-SHA-512 with authzid",
                        "SCRAM-SHA-512",
                        new InitialResponse(SASL_SCRAM_SHA512_CLIENT_INITIAL_WITH_AUTHZID),
                        List.of(new ChallengeResponse(
                                SASL_SCRAM_SHA_512_SERVER_FIRST,
                                SASL_SCRAM_SHA_512_CLIENT_FINAL),
                                new ChallengeResponse(
                                        SASL_SCRAM_SHA_512_SERVER_FINAL,
                                        null)),
                        "Ursel"),
                Arguments.argumentSet("SASL SCRAM-SHA-512 (username containing encoded comma)",
                        "SCRAM-SHA-512",
                        new InitialResponse("n,,n=test=2Cuser,r=rOprNGfwEbeRWgbNEkqO".getBytes(StandardCharsets.US_ASCII)),
                        List.of(new ChallengeResponse(
                                SASL_SCRAM_SHA_512_SERVER_FIRST,
                                SASL_SCRAM_SHA_512_CLIENT_FINAL),
                                new ChallengeResponse(
                                        SASL_SCRAM_SHA_512_SERVER_FINAL,
                                        null)),
                        "test,user"));
    }

    @ParameterizedTest
    @MethodSource("successfulSaslAuthentications")
    void shouldAuthenticateSuccessfully(String mechanism, InitialResponse initialResponse, List<ChallengeResponse> challengeResponses, String expectedAuthorizedId) {
        doAuthenticateSuccessfully(mechanism, initialResponse, challengeResponses);

        // Then
        verify(context).clientSaslAuthenticationSuccess(mechanism, expectedAuthorizedId);
        verify(context, never()).clientSaslAuthenticationFailure(anyString(), anyString(), nullable(Exception.class));

    }

    static Stream<Arguments> successfulSaslReauthentications() {
        return Stream.of(
                Arguments.argumentSet("reauth",
                        "PLAIN",
                        new InitialResponse(SASL_PLAIN_CLIENT_INITIAL),
                        List.of(new ChallengeResponse(new byte[0], null)),
                        new InitialResponse(SASL_PLAIN_CLIENT_INITIAL),
                        List.of(new ChallengeResponse(new byte[0], null)),
                        (Consumer<FilterContext>) context -> {
                            verify(context, never()).clientSaslAuthenticationFailure(anyString(), anyString(), nullable(Exception.class));
                            verify(context, times(2)).clientSaslAuthenticationSuccess("PLAIN", "tim");
                        }),
                Arguments.argumentSet("reauth changes of authzid",
                        "PLAIN",
                        new InitialResponse(SASL_PLAIN_CLIENT_INITIAL),
                        List.of(new ChallengeResponse(new byte[0], null)),
                        new InitialResponse("\0timmy\0tanstaaftanstaaf".getBytes(StandardCharsets.US_ASCII)),
                        List.of(new ChallengeResponse(new byte[0], null)),
                        (Consumer<FilterContext>) context -> {
                            verify(context, never()).clientSaslAuthenticationFailure(anyString(), anyString(), nullable(Exception.class));
                            verify(context).clientSaslAuthenticationSuccess("PLAIN", "tim");
                            verify(context).clientSaslAuthenticationSuccess("PLAIN", "timmy");
                        }));
    }

    @ParameterizedTest
    @MethodSource("successfulSaslReauthentications")
    void shouldReauthenticateSuccessfully(String mechanism, InitialResponse initialResponse, List<ChallengeResponse> challengeResponses,
                                          InitialResponse reauthInitialResponse, List<ChallengeResponse> reauthChallengeResponses, Consumer<FilterContext> verify) {
        doAuthenticateSuccessfully(mechanism, initialResponse, challengeResponses);
        doAuthenticateSuccessfully(mechanism, reauthInitialResponse, reauthChallengeResponses);
        verify.accept(context);
    }

    @Test
    void shouldDetectUnexpectedReauthentication() {
        // Given
        var filter = new SaslInspectionFilter(new Config(Set.of("PLAIN")));

        doSaslHandshakeRequest("PLAIN", filter);
        doSaslHandshakeResponse("PLAIN", filter);

        doSaslAuthenticateRequest(SASL_PLAIN_CLIENT_INITIAL, filter, (short) 0);
        doSaslAuthenticateResponse(new byte[0], filter);

        var unexpectedHandshake = new SaslHandshakeRequestData().setMechanism("PLAIN");
        var unexpectedHandshakeHeader = new RequestHeaderData().setRequestApiKey(unexpectedHandshake.apiKey());
        var expectedResponse = new SaslHandshakeResponseData().setErrorCode(Errors.ILLEGAL_SASL_STATE.code());

        // When

        var actualUpstreamHandshakeRequest = filter.onSaslHandshakeRequest(unexpectedHandshakeHeader.requestApiVersion(), unexpectedHandshakeHeader,
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

    private void doAuthenticateSuccessfully(String mechanism, InitialResponse initialResponse, List<ChallengeResponse> challengeResponses) {
        // Given
        var filter = new SaslInspectionFilter(new Config(Set.of(mechanism)));

        // When
        doSaslHandshakeRequest(mechanism, filter);
        doSaslHandshakeResponse(mechanism, filter);

        doSaslAuthenticateRequest(initialResponse.response(), filter);

        challengeResponses.forEach(cr -> {
            doSaslAuthenticateResponse(cr.challenge(), filter);
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

        var actualHandshakeRequest = filter.onSaslHandshakeRequest(handshakeRequestHeader.requestApiVersion(), handshakeRequestHeader,
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

    private void doSaslAuthenticateResponse(byte[] challenge, SaslInspectionFilter filter) {
        var authenticateResponse = new SaslAuthenticateResponseData().setAuthBytes(challenge);
        var expectedAuthenticateResponse = authenticateResponse.duplicate();

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
