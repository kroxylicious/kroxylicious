/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ResponseImplTest {

    @Test
    void shouldStoreHeaderAndBody() {
        var header = new ResponseHeaderData();
        var body = new FetchResponseData();

        var response = new ResponseImpl(header, body);

        assertThat(response.header()).isSameAs(header);
        assertThat(response.body()).isSameAs(body);
    }

    @Test
    void shouldRejectNullHeader() {
        FetchResponseData body = new FetchResponseData();
        assertThatThrownBy(() -> new ResponseImpl(null, body)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldRejectNullBody() {
        ResponseHeaderData header = new ResponseHeaderData();
        assertThatThrownBy(() -> new ResponseImpl(header, null)).isInstanceOf(NullPointerException.class);
    }
}
