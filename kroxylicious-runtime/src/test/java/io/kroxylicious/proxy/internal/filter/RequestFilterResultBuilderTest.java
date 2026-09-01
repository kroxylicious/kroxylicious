/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import java.time.Duration;
import java.util.stream.Stream;

import io.kroxylicious.kafka.common.message.ApiVersionsRequestData;
import io.kroxylicious.kafka.common.message.ApiVersionsResponseData;
import io.kroxylicious.kafka.common.message.FetchRequestData;
import io.kroxylicious.kafka.common.message.FetchResponseData;
import io.kroxylicious.kafka.common.message.LeaveGroupRequestData;
import io.kroxylicious.kafka.common.message.RequestHeaderData;
import io.kroxylicious.kafka.common.message.ResponseHeaderData;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;
import io.kroxylicious.kafka.common.protocol.Errors;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;
import io.kroxylicious.testing.filter.RequestFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RequestFilterResultBuilderTest {

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
        var future = builder.errorResponse(header, versionedMessage.apiMessage(), Errors.UNKNOWN_SERVER_ERROR).completed();

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
        var future = builder.errorResponse(header, new LeaveGroupRequestData(), Errors.UNKNOWN_SERVER_ERROR).completed();

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
        var future = builder.errorResponse(header, request.apiMessage(), Errors.UNKNOWN_SERVER_ERROR).completed();

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
        var future = builder.errorResponse(header, request.apiMessage(), Errors.UNKNOWN_SERVER_ERROR).completed();

        // Then
        assertThat(future)
                .succeedsWithin(Duration.ofSeconds(10))
                .satisfies(result -> {
                    assertThat(result.closeConnection()).describedAs("connection closed").isFalse();
                });
    }

    @Test
    void errorResponseFromErrorsSetsErrorCode() {
        // Given
        var header = apiVersionsHeader();

        // When
        var result = builder.errorResponse(header, new ApiVersionsRequestData(), Errors.INVALID_REQUEST).build();

        // Then
        assertThat(result.message())
                .asInstanceOf(InstanceOfAssertFactories.type(ApiVersionsResponseData.class))
                .satisfies(response -> assertThat(response.errorCode()).isEqualTo(Errors.INVALID_REQUEST.code()));
    }

    @Test
    void errorResponseFromErrorsWithMessageSetsErrorCode() {
        // Given
        var header = apiVersionsHeader();
        var message = "custom explanation";

        // When
        var result = builder.errorResponse(header, new ApiVersionsRequestData(), Errors.INVALID_REQUEST, message).build();

        // Then
        assertThat(result.message())
                .asInstanceOf(InstanceOfAssertFactories.type(ApiVersionsResponseData.class))
                .satisfies(response -> assertThat(response.errorCode()).isEqualTo(Errors.INVALID_REQUEST.code()));
    }

    @Test
    void errorResponseFromErrorsWithNullMessageUsesDefaultMessage() {
        // Given
        var header = apiVersionsHeader();

        // When
        var fromNullMessage = builder.errorResponse(header, new ApiVersionsRequestData(), Errors.INVALID_REQUEST, null).build();
        var fromNoMessage = new RequestFilterResultBuilderImpl()
                .errorResponse(header, new ApiVersionsRequestData(), Errors.INVALID_REQUEST).build();

        // Then
        assertThat(fromNullMessage.message()).isEqualTo(fromNoMessage.message());
    }

    @Test
    void errorResponseFromErrorsRejectsNone() {
        // Given
        var header = apiVersionsHeader();
        var requestMessage = new ApiVersionsRequestData();

        // When / Then
        assertThatThrownBy(() -> builder.errorResponse(header, requestMessage, Errors.NONE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void errorResponseFromErrorsWithMessageRejectsNone() {
        // Given
        var header = apiVersionsHeader();
        var requestMessage = new ApiVersionsRequestData();

        // When / Then
        assertThatThrownBy(() -> builder.errorResponse(header, requestMessage, Errors.NONE, "some message"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void errorResponseFromErrorsRejectsNullError() {
        // Given
        var header = apiVersionsHeader();
        var requestMessage = new ApiVersionsRequestData();

        // When / Then
        assertThatThrownBy(() -> builder.errorResponse(header, requestMessage, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void errorResponseFromErrorsWithMessageRejectsNullError() {
        // Given
        var header = apiVersionsHeader();
        var requestMessage = new ApiVersionsRequestData();

        // When / Then
        assertThatThrownBy(() -> builder.errorResponse(header, requestMessage, null, "some message"))
                .isInstanceOf(NullPointerException.class);
    }

    private static RequestHeaderData apiVersionsHeader() {
        var header = new RequestHeaderData();
        header.setRequestApiKey(ApiKeys.API_VERSIONS.id);
        header.setRequestApiVersion(ApiKeys.API_VERSIONS.latestVersion());
        header.setCorrelationId(23456);
        return header;
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
