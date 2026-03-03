/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.time.Duration;
import java.util.stream.Stream;

import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.test.RequestFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RequestFilterResultBuilderTest {

    private static final UnknownServerException FILTER_RUNTIME_EXCEPTION = new UnknownServerException("Filter says yeah, nah!");
    private final RequestFilterResultBuilder builder = new RequestFilterResultBuilderImpl();

    @Test
    void forwardRequest() {
        var request = new FetchRequestData();
        var header = new RequestHeaderData();
        var result = builder.forward(header, request).build();
        assertThat(result.message()).isEqualTo(request);
        assertThat(result.header()).isEqualTo(header);
        assertThat(result.closeConnection()).isFalse();
        assertThat(result.drop()).isFalse();
    }

    @Test
    void forwardRejectResponseData() {
        var res = new FetchResponseData();
        var header = new RequestHeaderData();
        assertThatThrownBy(() -> builder.forward(header, res)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void forwardRejectNullResponseData() {
        var header = new RequestHeaderData();
        assertThatThrownBy(() -> builder.forward(header, null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void forwardRejectsNullHeader() {
        var req = new FetchRequestData();
        assertThatThrownBy(() -> builder.forward(null, req)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void bareCloseConnection() {
        var result = builder.withCloseConnection().build();
        assertThat(result.closeConnection()).isTrue();
    }

    @Test
    void forwardWithCloseConnection() {
        var request = new FetchRequestData();
        var header = new RequestHeaderData();

        var result = builder.forward(header, request).withCloseConnection().build();
        assertThat(result.message()).isEqualTo(request);
        assertThat(result.header()).isEqualTo(header);
        assertThat(result.closeConnection()).isTrue();
    }

    @Test
    void shortCircuit() {
        var res = new FetchResponseData();
        var result = builder.shortCircuitResponse(res).build();
        assertThat(result.message()).isEqualTo(res);
        assertThat(result.header()).isNull();
        assertThat(result.closeConnection()).isFalse();
    }

    @Test
    void shortCircuitResultWithCloseConnection() {
        var res = new FetchResponseData();
        var result = builder.shortCircuitResponse(res).withCloseConnection().build();
        assertThat(result.message()).isEqualTo(res);
        assertThat(result.header()).isNull();
        assertThat(result.closeConnection()).isTrue();
    }

    @Test
    void shortCircuitHeaderAndResponseData() {
        var res = new FetchResponseData();
        var header = new ResponseHeaderData();
        var result = builder.shortCircuitResponse(header, res).build();
        assertThat(result.message()).isEqualTo(res);
        assertThat(result.header()).isEqualTo(header);
        assertThat(result.closeConnection()).isFalse();
    }

    @Test
    void shortCircuitRejectsRequestData() {
        var req = new FetchRequestData();
        assertThatThrownBy(() -> builder.shortCircuitResponse(req)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shortCircuitRejectsNullRequestData() {
        assertThatThrownBy(() -> builder.shortCircuitResponse(null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void drop() {
        var result = builder.drop().build();
        assertThat(result.drop()).isTrue();
        assertThat(result.message()).isNull();
        assertThat(result.header()).isNull();
    }

    @Test
    void completedApi() throws Exception {
        var request = new FetchRequestData();
        var header = new RequestHeaderData();
        var future = builder.forward(header, request).completed();
        assertThat(future).isCompleted();
        var result = future.toCompletableFuture().get();
        assertThat(result.message()).isEqualTo(request);
        assertThat(result.header()).isEqualTo(header);
    }

    @ParameterizedTest
    @MethodSource({ "latestVersions", "oldestVersions" })
    void shouldBuildErrorResponse(RequestFactory.ApiMessageVersion versionedMessage) {
        // Given
        final ApiKeys apiKey = versionedMessage.getApiKey();
        final Class<? extends ApiMessage> responseMessageClass = apiKey.messageType.newResponse().getClass();
        var header = new RequestHeaderData();
        header.setRequestApiKey(apiKey.id);
        header.setRequestApiVersion(versionedMessage.apiVersion());
        header.setCorrelationId(23456);

        // When
        var future = builder.errorResponse(header, versionedMessage.apiMessage(), FILTER_RUNTIME_EXCEPTION).completed();

        // Then
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(10))
                .satisfies(result -> {
                    assertThat(result.header())
                            .satisfies(headerMessage -> assertThat(headerMessage).asInstanceOf(InstanceOfAssertFactories.type(ResponseHeaderData.class))
                                    .satisfies(responseHeaderData -> assertThat(responseHeaderData.correlationId()).isEqualTo(header.correlationId())));
                    assertThat(result.message()).satisfies(actualResponse -> assertThat(actualResponse).isExactlyInstanceOf(responseMessageClass));
                });
    }

    @Test
    void shouldBuildErrorResponseForIllegitimateLeaveGroupRequest() {
        // Given
        final ApiKeys apiKey = ApiKeys.LEAVE_GROUP;
        final Class<? extends ApiMessage> responseMessageClass = apiKey.messageType.newResponse().getClass();
        var header = new RequestHeaderData();
        header.setRequestApiKey(apiKey.id);
        header.setRequestApiVersion(apiKey.latestVersion());
        header.setCorrelationId(23456);

        // When
        var future = builder.errorResponse(header, new LeaveGroupRequestData(), FILTER_RUNTIME_EXCEPTION).completed();

        // Then
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(10))
                .satisfies(result -> {
                    assertThat(result.header())
                            .satisfies(headerMessage -> assertThat(headerMessage).asInstanceOf(InstanceOfAssertFactories.type(ResponseHeaderData.class))
                                    .satisfies(responseHeaderData -> assertThat(responseHeaderData.correlationId()).isEqualTo(header.correlationId())));
                    assertThat(result.message()).satisfies(actualResponse -> assertThat(actualResponse).isExactlyInstanceOf(responseMessageClass));
                });
    }

    @Test
    void shouldErrorResponseShouldNotInvokeRemainingFilterChain() {
        // Given
        var request = RequestFactory.apiMessageFor(ApiKeys::latestVersion, ApiKeys.PRODUCE).findFirst().orElseThrow(IllegalStateException::new);
        var header = new RequestHeaderData();
        header.setRequestApiKey(request.apiMessage().apiKey());
        header.setRequestApiVersion(request.apiVersion());
        header.setCorrelationId(23456);

        // When
        var future = builder.errorResponse(header, request.apiMessage(), FILTER_RUNTIME_EXCEPTION).completed();

        // Then
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(10))
                .satisfies(result -> {
                    assertThat(result.shortCircuitResponse()).describedAs("request did not short circuit").isTrue();
                    assertThat(result.drop()).describedAs("request dropped").isFalse();
                });
    }

    @Test
    void shouldErrorResponseShouldNotCloseConnection() {
        // Given
        var request = RequestFactory.apiMessageFor(ApiKeys::latestVersion, ApiKeys.PRODUCE).findFirst().orElseThrow(IllegalStateException::new);
        var header = new RequestHeaderData();
        header.setRequestApiKey(request.apiMessage().apiKey());
        header.setRequestApiVersion(request.apiVersion());
        header.setCorrelationId(23456);

        // When
        var future = builder.errorResponse(header, request.apiMessage(), FILTER_RUNTIME_EXCEPTION).completed();

        // Then
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(10))
                .satisfies(result -> {
                    assertThat(result.closeConnection()).describedAs("connection closed").isFalse();
                });
    }

    public static Stream<Arguments> latestVersions() {
        return RequestFactory
                .apiMessageFor(ApiKeys::latestVersion)
                .map(versionedMessage -> Named.named(versionedMessage.getApiKey().name() + "@v" + versionedMessage.apiMessage().highestSupportedVersion(),
                        versionedMessage))
                .map(Arguments::of);
    }

    public static Stream<Arguments> oldestVersions() {
        return RequestFactory
                .apiMessageFor(ApiKeys::oldestVersion)
                .map(versionedMessage -> Named.named(versionedMessage.getApiKey().name() + "@v" + versionedMessage.apiMessage().lowestSupportedVersion(),
                        versionedMessage))
                .map(Arguments::of);
    }
}
