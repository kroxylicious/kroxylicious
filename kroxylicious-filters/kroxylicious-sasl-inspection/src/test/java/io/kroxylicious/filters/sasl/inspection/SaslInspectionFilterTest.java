/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

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
import io.kroxylicious.proxy.filter.filterresultbuilder.CloseOrTerminalStage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SaslInspectionFilterTest {

    public static final String PROXY_PROBE_MECHANISM = "BOGUS";
    @Mock(strictness = LENIENT)
    private FilterContext context;

    @Captor
    private ArgumentCaptor<ApiMessage> apiMessageCaptor;

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
        var filter = new SaslInspectionFilter(new Config(true, Set.of("PLAIN")));

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

    @Test
    void shouldReturnHandshakeResponseDownstreamWhenMechanismsAgree() {
        // Given
        var filter = new SaslInspectionFilter(new Config(true, Set.of("PLAIN")));

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

        // When
        var actualDownstreamHandshakeResponse = filter.onSaslHandshakeResponse(upstreamHandshakeResponse.highestSupportedVersion(), upstreamHandshakeResponseHeader,
                upstreamHandshakeResponse, context);

        // Then
        assertThat(actualDownstreamHandshakeResponse)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedDownstreamHandshakeResponse));

    }

    @Test
    void shouldReturnHandshakeErrorResponseDownstreamWhenClientMechanismUnknownToProxy() {
        // Given
        var filter = new SaslInspectionFilter(new Config(true, Set.of("PLAIN")));

        var downstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism("NOTAMECH");
        var downstreamHandshakeRequestHeader = new RequestHeaderData().setRequestApiKey(downstreamHandshakeRequest.apiKey())
                .setRequestApiVersion(downstreamHandshakeRequest.highestSupportedVersion());
        var expectedUpstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism(PROXY_PROBE_MECHANISM);

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
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedDownstreamHandshakeResponse));
    }

    @Test
    void shouldReturnHandshakeErrorResponseDownstreamWhenClientMechanismUnknownToBroker() {
        // Given
        var filter = new SaslInspectionFilter(new Config(true, Set.of("PLAIN", "SCRAM-SHA-256")));

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
        var expectedDownstreamHandshakeResponse = new SaslHandshakeResponseData().setMechanisms(List.of("SCRAM-SHA-256")).setErrorCode(Errors.UNSUPPORTED_SASL_MECHANISM.code());

        // When
        var forwardedHandshakeResponse = filter.onSaslHandshakeResponse(upstreamHandshakeResponse.highestSupportedVersion(), upstreamHandshakeResponseHeader,
                upstreamHandshakeResponse, context);

        // Then
        assertThat(forwardedHandshakeResponse)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedDownstreamHandshakeResponse));
    }

    static Stream<Arguments> successfulSaslAuthentications() {
        return Stream.of(
                Arguments.argumentSet("SASL PLAIN (response provides authcid only)", "PLAIN", "\0tim\0tanstaaftanstaaf".getBytes(StandardCharsets.US_ASCII), "tim"),
                Arguments.argumentSet("SASL PLAIN (response provides authzid and authcid)", "PLAIN", "Ursel\0Kurt\0xipj3plmq".getBytes(StandardCharsets.US_ASCII),
                        "Ursel"));
    }

    @ParameterizedTest
    @MethodSource("successfulSaslAuthentications")
    void shouldAuthenticateSuccessfully(String mechanism, byte[] initialResponse, String expectedAuthorizedId) {
        // Given
        var filter = new SaslInspectionFilter(new Config(true, Set.of(mechanism)));

        var downstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism(mechanism);
        var downstreamHandshakeRequestHeader = new RequestHeaderData().setRequestApiKey(downstreamHandshakeRequest.apiKey())
                .setRequestApiVersion(downstreamHandshakeRequest.highestSupportedVersion());
        var expectedUpstreamHandshakeRequest = downstreamHandshakeRequest.duplicate();

        var actualUpstreamHandshakeRequest = filter.onSaslHandshakeRequest(downstreamHandshakeRequest.highestSupportedVersion(), downstreamHandshakeRequestHeader,
                downstreamHandshakeRequest, context);

        assertThat(actualUpstreamHandshakeRequest)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedUpstreamHandshakeRequest));

        var upstreamHandshakeResponse = new SaslHandshakeResponseData().setMechanisms(List.of(mechanism));
        var upstreamHandshakeResponseHeader = new ResponseHeaderData();
        var expectedDownstreamHandshakeResponse = upstreamHandshakeResponse.duplicate();

        var actualDownstreamHandshakeResponse = filter.onSaslHandshakeResponse(upstreamHandshakeResponse.highestSupportedVersion(), upstreamHandshakeResponseHeader,
                upstreamHandshakeResponse, context);

        assertThat(actualDownstreamHandshakeResponse)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedDownstreamHandshakeResponse));

        var downstreamAuthenticateRequest = new SaslAuthenticateRequestData().setAuthBytes(initialResponse);
        var downstreamAuthenticateRequestHeader = new RequestHeaderData().setRequestApiKey(downstreamAuthenticateRequest.apiKey())
                .setRequestApiVersion(downstreamAuthenticateRequest.highestSupportedVersion());
        var expectedUpstreamAuthenticateRequest = downstreamAuthenticateRequest.duplicate();

        var actualUpstreamAuthenticateRequest = filter.onSaslAuthenticateRequest(downstreamAuthenticateRequest.highestSupportedVersion(),
                downstreamAuthenticateRequestHeader, downstreamAuthenticateRequest, context);

        assertThat(actualUpstreamAuthenticateRequest)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedUpstreamAuthenticateRequest));

        // When
        var upstreamAuthenticateResponse = new SaslAuthenticateResponseData();
        var upstreamAuthenticateResponseHeader = new ResponseHeaderData();
        var expectedDownstreamAuthenticateResponse = upstreamAuthenticateResponse.duplicate();

        var actualDownstreamAuthenticateResponse = filter.onSaslAuthenticateResponse(upstreamAuthenticateResponse.highestSupportedVersion(),
                upstreamAuthenticateResponseHeader,
                upstreamAuthenticateResponse, context);

        // Then

        assertThat(actualDownstreamAuthenticateResponse)
                .succeedsWithin(Duration.ofSeconds(1))
                .satisfies(rfr -> assertThat(rfr.message())
                        .isEqualTo(expectedDownstreamAuthenticateResponse));

        verify(context).clientSaslAuthenticationSuccess(mechanism, expectedAuthorizedId);
        verify(context, never()).clientSaslAuthenticationFailure(anyString(), anyString(), nullable(Exception.class));

    }

}