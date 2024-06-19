/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.bytebuf;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.record.Record;

import io.apicurio.registry.resolver.strategy.ArtifactReference;
import io.apicurio.schema.validation.json.JsonValidationResult;
import io.apicurio.schema.validation.json.JsonValidator;

import io.kroxylicious.proxy.filter.schema.validation.Result;

public class JsonSchemaBytebufValidator implements BytebufValidator {

    private final JsonValidator jsonValidator;

    public JsonSchemaBytebufValidator(Map<String, Object> schemaResolverConfig, Long globalId) {
        this.jsonValidator = new JsonValidator(schemaResolverConfig, Optional.of(ArtifactReference.fromGlobalId(globalId)));
    }

    @Override
    public CompletionStage<Result> validate(ByteBuffer buffer, int length, Record record, boolean isKey) {
        try {
            JsonValidationResult jsonValidationResult = jsonValidator.validateByArtifactReference(buffer.clear());
            return jsonValidationResult.success() ? Result.VALID
                    : CompletableFuture.completedFuture(new Result(false, jsonValidationResult.toString()));
        }
        catch (RuntimeException ex) {
            return CompletableFuture.completedFuture(new Result(false, ex.getMessage()));
        }
    }
}
