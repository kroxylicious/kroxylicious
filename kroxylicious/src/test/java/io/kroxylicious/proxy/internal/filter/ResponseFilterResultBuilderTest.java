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

public class ResponseFilterResultBuilderTest {

    private ResponseFilterResultBuilder builder = new ResponseFilterResultBuilderImpl();

    @Test
    public void responseResult() {
        var res = new FetchResponseData();
        builder.withMessage(res);
        var result = builder.build();
        assertThat(result.message()).isEqualTo(res);
        assertThat(result.header()).isNull();
        assertThat(result.closeConnection()).isFalse();
    }

    @Test
    public void responseResultWithHeader() {
        var res = new FetchResponseData();
        var head = new ResponseHeaderData();
        builder.withMessage(res);
        builder.withHeader(head);
        var result = builder.build();
        assertThat(result.message()).isEqualTo(res);
        assertThat(result.header()).isEqualTo(head);
    }

    @Test
    public void rejectsRequestData() {
        var req = new FetchRequestData();
        assertThatThrownBy(() -> builder.withMessage(req)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void closeConnection() {
        builder.withCloseConnection(true);
        var result = builder.build();
        assertThat(result.closeConnection()).isTrue();
    }

}
