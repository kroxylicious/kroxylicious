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

    private ResponseFilterResultBuilder builder = new ResponseFilterResultBuilderImpl();

    @Test
    void forwardResponse() {
        var res = new FetchResponseData();
        var head = new ResponseHeaderData();
        builder.forward(head, res);
        var result = builder.build();
        assertThat(result.message()).isEqualTo(res);
        assertThat(result.header()).isEqualTo(head);
        assertThat(result.closeConnection()).isFalse();
        assertThat(result.drop()).isFalse();
    }

    @Test
    void forwardRejectsRequestData() {
        var req = new FetchRequestData();
        var head = new ResponseHeaderData();
        assertThatThrownBy(() -> builder.forward(head, req)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void bareCloseConnection() {
        builder.withCloseConnection2(true);
        var result = builder.build();
        assertThat(result.closeConnection()).isTrue();
    }

    @Test
    void forwardResponseWithCloseConnection() {
        var res = new FetchResponseData();
        var head = new ResponseHeaderData();
        builder.forward(head, res).withCloseConnection2(true);
        var result = builder.build();
        assertThat(result.message()).isEqualTo(res);
        assertThat(result.header()).isEqualTo(head);
        assertThat(result.closeConnection()).isTrue();
    }

    @Test
    void drop() {
        var result = builder.drop().build();
        assertThat(result.drop()).isTrue();
        assertThat(result.message()).isNull();
        assertThat(result.header()).isNull();
    }

}
