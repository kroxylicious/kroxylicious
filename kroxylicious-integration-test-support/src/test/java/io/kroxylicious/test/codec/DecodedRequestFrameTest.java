/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.codec;

import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class DecodedRequestFrameTest {

    // testing this sanity check is hard as the implementation should always write the correct amount of bytes to the buffer
    // so we have to resort to mocking for some evidence that it is checking something
    @Test
    void frameEncodeFailsIfUnexpectedAmountOfBytesWrittenToAccessor() {
        DecodedRequestFrame<ApiVersionsRequestData> frame = new DecodedRequestFrame<>(ApiKeys.API_VERSIONS.latestVersion(), 5,
                new RequestHeaderData(), new ApiVersionsRequestData(), ApiKeys.API_VERSIONS.latestVersion());
        ByteBufAccessor mockAccessor = Mockito.mock(ByteBufAccessor.class);
        int encodedSize = frame.estimateEncodedSize();
        assertThat(encodedSize).isGreaterThan(0);
        int mockWrittenBytes = encodedSize - 1;
        when(mockAccessor.writerIndex()).thenReturn(0, mockWrittenBytes); // simulate less date being written to the buffer than expected
        assertThatThrownBy(() -> frame.encode(mockAccessor)).isInstanceOf(IllegalStateException.class)
                .hasMessage("written bytes " + mockWrittenBytes + " != encoded size " + encodedSize);
    }

}