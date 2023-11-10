/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.bytebuf;

import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteBufferInputStream;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.proxy.filter.schema.validation.Result;

/**
 * Checks if a Record's value is well-formed JSON, optionally checking if
 * Object keys are unique. Object key uniqueness is not a hard requirement
 * in the spec but some consumer implementations may expect them to be unique.
 */
class JsonSyntaxBytebufValidator implements BytebufValidator {
    private final boolean validateObjectKeysUnique;

    static final ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    JsonSyntaxBytebufValidator(boolean validateObjectKeysUnique) {
        this.validateObjectKeysUnique = validateObjectKeysUnique;
    }

    @Override
    public Result validate(ByteBuffer buffer, int size, Record record, boolean isKey) {
        if (buffer == null) {
            throw new IllegalArgumentException("buffer is null");
        }
        if (size < 1) {
            throw new IllegalArgumentException("size is less than 1");
        }
        try (InputStream inputStream = new ByteBufferInputStream(buffer);
                JsonParser parser = mapper.getFactory().createParser(inputStream)) {
            if (validateObjectKeysUnique) {
                parser.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
            }
            while (parser.nextToken() != null) {
            }
            return Result.VALID;
        }
        catch (Exception e) {
            String message = "value was not syntactically correct JSON" + (e.getMessage() != null ? ": " + e.getMessage() : "");
            return new Result(false, message);
        }
    }

}
