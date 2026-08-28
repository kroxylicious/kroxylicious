/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kafka.message.json;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kafka.common.message.ApiMessageType;
import io.kroxylicious.kafka.common.message.MetadataRequestData;

import static org.assertj.core.api.Assertions.assertThat;

class VendoredKafkaApiMessageConverterTest {

    @Test
    void roundTripsThroughJson() {
        var original = new MetadataRequestData().setAllowAutoTopicCreation(false).setIncludeClusterAuthorizedOperations(true);
        var converter = VendoredKafkaApiMessageConverter.requestConverterFor(ApiMessageType.METADATA);

        var json = converter.writer().apply(original, (short) 9);
        var roundTripped = converter.reader().apply(json, (short) 9);

        assertThat(roundTripped).isEqualTo(original);
    }
}
