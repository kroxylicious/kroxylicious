/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.api.common;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ProtocolTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void serialisation() throws JsonProcessingException {
        assertThat(objectMapper.writeValueAsString(Protocol.TCP)).isEqualTo("\"TCP\"");
    }

    @Test
    void deserialisation() throws JsonProcessingException {
        assertThat(objectMapper.readValue("\"TLS\"", Protocol.class)).isEqualTo(Protocol.TLS);
    }
}