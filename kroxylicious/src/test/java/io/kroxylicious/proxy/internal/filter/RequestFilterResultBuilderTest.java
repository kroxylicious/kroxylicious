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

import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.kroxylicious.proxy.filter.RequestFilterResultBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RequestFilterResultBuilderTest {

    private RequestFilterResultBuilder builder = new RequestFilterResultBuilderImpl();

    @Test
    public void requestResult() {
        var req = new FetchRequestData();
        builder.withMessage(req);
        var result = builder.build();
        assertThat(result.message()).isEqualTo(req);
        assertThat(result.header()).isNull();
        assertThat(result.closeConnection()).isFalse();
    }

    @Test
    public void requestResultWithHeader() {
        var req = new FetchRequestData();
        var head = new RequestHeaderData();
        builder.withMessage(req);
        builder.withHeader(head);
        var result = builder.build();
        assertThat(result.message()).isEqualTo(req);
        assertThat(result.header()).isEqualTo(head);
    }

    @Test
    public void rejectsResponseData() {
        var res = new FetchResponseData();
        assertThatThrownBy(() -> builder.withMessage(res)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void rejectsResponseHeaderData() {
        var head = new ResponseHeaderData();
        assertThatThrownBy(() -> builder.withHeader(head)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void closeConnection() {
        builder.withCloseConnection(true);
        var result = builder.build();
        assertThat(result.closeConnection()).isTrue();
    }

    @Test
    public void shortCircuitResult() {
        var res = new FetchResponseData();
        builder.asRequestShortCircuitResponse().withMessage(res);
        var result = builder.build();
        assertThat(result).isInstanceOf(RequestFilterResult.class);
        assertThat(result.message()).isEqualTo(res);
        assertThat(result.header()).isNull();
        assertThat(result.closeConnection()).isFalse();
    }

    @Test
    public void shortCircuitRejectsRequestData() {
        var req = new FetchRequestData();
        assertThatThrownBy(() -> builder.asRequestShortCircuitResponse().withMessage(req)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void shortCircuitRejectsRequestHeaderData() {
        var header = new RequestHeaderData();
        assertThatThrownBy(() -> builder.asRequestShortCircuitResponse().withHeader(header)).isInstanceOf(IllegalArgumentException.class);
    }

}
