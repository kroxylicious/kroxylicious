/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity;

import java.util.Arrays;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaSerdesTest {

    // v2 RequestHeaderData wire layout, computed independently of KafkaSerdes/RequestHeaderData.write():
    // requestApiKey(short)=18, requestApiVersion(short)=7, correlationId(int)=0x01020304,
    // clientId="krox" (length-prefixed), then a v2 tagged-fields count varint of 0.
    private static final byte[] KNOWN_BYTES = {
            0x00, 0x12,
            0x00, 0x07,
            0x01, 0x02, 0x03, 0x04,
            0x00, 0x04, 'k', 'r', 'o', 'x',
            0x00
    };

    // Same as KNOWN_BYTES, but the trailing tagged-fields count is 1 instead of 0, followed by one
    // unknown tagged field: tag=1, size=1, data=[0x01].
    private static final byte[] KNOWN_BYTES_WITH_UNKNOWN_TAGGED_FIELD = {
            0x00, 0x12,
            0x00, 0x07,
            0x01, 0x02, 0x03, 0x04,
            0x00, 0x04, 'k', 'r', 'o', 'x',
            0x01, 0x01, 0x01, 0x01
    };

    @Test
    void shouldWriteKnownByteLayout() {
        // Given
        RequestHeaderData original = new RequestHeaderData()
                .setRequestApiKey((short) 18)
                .setRequestApiVersion((short) 7)
                .setCorrelationId(0x01020304)
                .setClientId("krox");

        // When
        byte[] bytes = KafkaSerdes.write(original, (short) 2);

        // Then
        assertThat(bytes).isEqualTo(KNOWN_BYTES);
    }

    @Test
    void shouldDecodeInlineWithSpecifiedVersion() {
        // When
        ReadResult<RequestHeaderData> result = KafkaSerdes.read(new RequestHeaderData(), KNOWN_BYTES, (short) 1);

        // Then
        // KNOWN_BYTES is v2-shaped (it has a trailing tagged-fields count); reading it as v1 correctly
        // stops before that byte, since v1 has no tagged-fields section - this is exactly the signal
        // assertAllBytesConsumed() relies on to catch a version mismatch.
        assertThat(result.unreadBytes()).isEqualTo(1);
        assertThat(result.message()).satisfies(requestHeaderData -> {
            assertThat(requestHeaderData.requestApiKey()).isEqualTo((short) 18);
            assertThat(requestHeaderData.requestApiVersion()).isEqualTo((short) 7);
            assertThat(requestHeaderData.correlationId()).isEqualTo(0x01020304);
            assertThat(requestHeaderData.clientId()).isEqualTo("krox");
        });
    }

    @Test
    void shouldReadKnownByteLayoutWithUnknownTaggedField() {
        // When
        ReadResult<RequestHeaderData> result = KafkaSerdes.read(new RequestHeaderData(), KNOWN_BYTES_WITH_UNKNOWN_TAGGED_FIELD, (short) 2);

        // Then
        assertThat(result.unreadBytes()).isZero();
        assertThat(result.message()).satisfies(requestHeaderData -> {
            assertThat(requestHeaderData.requestApiKey()).isEqualTo((short) 18);
            assertThat(requestHeaderData.requestApiVersion()).isEqualTo((short) 7);
            assertThat(requestHeaderData.correlationId()).isEqualTo(0x01020304);
            assertThat(requestHeaderData.clientId()).isEqualTo("krox");
            assertThat(requestHeaderData.unknownTaggedFields()).containsExactly(new RawTaggedField(1, new byte[]{ 1 }));
        });
    }

    @Test
    void shouldFailForMalformedInput() {
        // When
        ReadResult<RequestHeaderData> result = KafkaSerdes.read(new RequestHeaderData(), Arrays.copyOfRange(KNOWN_BYTES, 1, KNOWN_BYTES.length), (short) 2);

        // Then
        assertThat(result.unreadBytes()).isEqualTo(KNOWN_BYTES.length - 1);
        assertThat(result.error()).isInstanceOf(RuntimeException.class).hasMessageContaining("Error reading byte array");
    }
}