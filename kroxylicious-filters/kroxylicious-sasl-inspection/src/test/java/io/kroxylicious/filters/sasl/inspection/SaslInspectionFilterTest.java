/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filters.sasl.inspection;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SaslInspectionFilterTest {

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
    void shouldForwardHandshakeRequestWithSupportedMechanism() {
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
    void shouldReturnHandshakeResponseGivenRequestWithSupportedMechanism() {
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
    void shouldReturnHandshakeResponseWithIntersectedSupportMechsGivenRequestWithUnsupportedMechanism() {
        // Given
        var filter = new SaslInspectionFilter(new Config(true, Set.of("PLAIN")));

        var downstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism("NOTAMECH");
        var downstreamHandshakeRequestHeader = new RequestHeaderData().setRequestApiKey(downstreamHandshakeRequest.apiKey())
                .setRequestApiVersion(downstreamHandshakeRequest.highestSupportedVersion());
        var expectedUpstreamHandshakeRequest = new SaslHandshakeRequestData().setMechanism("BOGUS");

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
    void successfulSaslPlainAuth() {
        var filter = new SaslInspectionFilter(new Config(true, Set.of("PLAIN")));

        var handshakeRequest = new SaslHandshakeRequestData().setMechanism("PLAIN");
        var handshakeHeader = new RequestHeaderData().setRequestApiKey(handshakeRequest.apiKey()).setRequestApiVersion(handshakeRequest.highestSupportedVersion());
        var forwardedHandshakeRequest = filter.onSaslHandshakeRequest(handshakeRequest.highestSupportedVersion(), handshakeHeader, handshakeRequest, context);

        assertThat(forwardedHandshakeRequest)
                .succeedsWithin(Duration.ofSeconds(1))
                .extracting(RequestFilterResult::message)
                .isEqualTo(handshakeRequest);
    }
}