/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.model;

import java.io.IOException;
import java.nio.file.Path;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.kroxylicious.krpccodegen.schema.MessageSpec;

/**
 * The Kafka message spec parser.
 */
public class MessageSpecParser {

    private static final ObjectMapper JSON_SERDE = new ObjectMapper();

    static {
        JSON_SERDE.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        JSON_SERDE.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        JSON_SERDE.configure(DeserializationFeature.FAIL_ON_TRAILING_TOKENS, true);
        JSON_SERDE.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        JSON_SERDE.setDefaultPropertyInclusion(JsonInclude.Include.NON_EMPTY);
    }

    /**
     * Parsers the given spec and produces a {@link MessageSpec} that represents it.
     *
     * @param messageSpec path of message spec file
     * @return message specification
     * @throws IOException i/o exception processing the file
     */
    public MessageSpec getMessageSpec(Path messageSpec) throws IOException {
        return JSON_SERDE.readValue(messageSpec.toFile(), MessageSpec.class);
    }
}
