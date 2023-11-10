/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;

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

}
