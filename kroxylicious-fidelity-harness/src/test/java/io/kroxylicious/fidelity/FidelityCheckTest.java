/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.fidelity;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kafka.common.message.RequestHeaderData;

import static org.assertj.core.api.Assertions.assertThat;

class FidelityCheckTest {

    @Test
    void kroxyliciousShouldReadKafkaSerialisedMessage() {
        // Given
        io.kroxylicious.kafka.common.message.RequestHeaderData kafkaSource = new io.kroxylicious.kafka.common.message.RequestHeaderData()
                .setRequestApiKey((short) 18)
                .setRequestApiVersion((short) 7)
                .setCorrelationId(0x01020304)
                .setClientId("kroxylicious-client");

        // When
        ReadResult<RequestHeaderData> result = FidelityCheck.kroxyliciousReads(kafkaSource, new RequestHeaderData(), (short) 2);

        // Then
        assertThat(result.error()).isNull();
        assertThat(result.unreadBytes()).isZero();
        assertThat(result.message()).usingRecursiveComparison().isEqualTo(kafkaSource);
    }

    @Test
    void kafkaShouldReadKroxyliciousSerialisedMessage() {
        // Given
        RequestHeaderData oursSource = new RequestHeaderData()
                .setRequestApiKey((short) 18)
                .setRequestApiVersion((short) 7)
                .setCorrelationId(0x01020304)
                .setClientId("kroxylicious-client");

        // When
        ReadResult<io.kroxylicious.kafka.common.message.RequestHeaderData> result = FidelityCheck.kafkaReads(
                oursSource, new io.kroxylicious.kafka.common.message.RequestHeaderData(), (short) 2);

        // Then
        assertThat(result.error()).isNull();
        assertThat(result.unreadBytes()).isZero();
        assertThat(result.message()).usingRecursiveComparison().isEqualTo(oursSource);
    }

    @Test
    void kroxyliciousShouldFailTheSameWayKafkaDoesOnMalformedInput() {
        // Given - a clientId length prefix (100) far exceeding the 0 bytes actually remaining
        byte[] malformed = { 0x00, 0x12, 0x00, 0x07, 0x01, 0x02, 0x03, 0x04, 0x00, 0x64 };

        // When
        FidelityCheck.ErrorParity parity = FidelityCheck.compareErrorHandling(
                malformed, new io.kroxylicious.kafka.common.message.RequestHeaderData(), new RequestHeaderData(), (short) 2);

        // Then
        parity.assertEquivalentResults();
    }
}
