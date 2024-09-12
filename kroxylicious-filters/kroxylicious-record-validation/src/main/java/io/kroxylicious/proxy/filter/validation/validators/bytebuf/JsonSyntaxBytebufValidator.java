/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.bytebuf;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.utils.ByteBufferInputStream;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.proxy.filter.validation.validators.Result;

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
    public CompletionStage<Result> validate(ByteBuffer buffer, Record record, boolean isKey) {
        Objects.requireNonNull(record);
        if (buffer == null) {
            throw new IllegalArgumentException("buffer is null");
        }
        if (buffer.remaining() < 1) {
            throw new IllegalArgumentException("size is less than 1");
        }

        try (InputStream inputStream = new ByteBufferInputStream(buffer);
                JsonParser parser = mapper.getFactory().createParser(inputStream)) {
            if (validateObjectKeysUnique) {
                parser.enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
            }
            while (parser.nextToken() != null) {
            }

            return Result.VALID_RESULT_STAGE;
        }
        catch (Exception e) {
            String message = "value was not syntactically correct JSON" + (e.getMessage() != null ? ": " + e.getMessage() : "");
            return CompletableFuture.completedFuture(new Result(false, message));
        }
    }
}
