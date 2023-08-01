/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.filter;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.filter.ResponseFilterResultBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResponseFilterResultBuilderTest {

    private final ResponseFilterResultBuilder builder = new ResponseFilterResultBuilderImpl();

    @Test
    void forwardResponse() {
        var res = new FetchResponseData();
        var header = new ResponseHeaderData();
        var result = builder.forward(header, res).build();
        assertThat(result.message()).isEqualTo(res);
        assertThat(result.header()).isEqualTo(header);
        assertThat(result.closeConnection()).isFalse();
        assertThat(result.drop()).isFalse();
    }

    @Test
    void forwardRejectsRequestData() {
        var req = new FetchRequestData();
        var header = new ResponseHeaderData();
        assertThatThrownBy(() -> builder.forward(header, req)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void forwardRejectsNullRequestData() {
        var header = new ResponseHeaderData();
        assertThatThrownBy(() -> builder.forward(header, null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void bareCloseConnection() {
        var result = builder.withCloseConnection().build();
        assertThat(result.closeConnection()).isTrue();
    }

    @Test
    void forwardResponseWithCloseConnection() {
        var res = new FetchResponseData();
        var header = new ResponseHeaderData();
        var result = builder.forward(header, res).withCloseConnection().build();
        assertThat(result.message()).isEqualTo(res);
        assertThat(result.header()).isEqualTo(header);
        assertThat(result.closeConnection()).isTrue();
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
        var res = new FetchResponseData();
        var header = new ResponseHeaderData();
        var future = builder.forward(header, res).completed();
        assertThat(future).isCompleted();
        var result = future.toCompletableFuture().get();
        assertThat(result.message()).isEqualTo(res);
        assertThat(result.header()).isEqualTo(header);
    }
}
