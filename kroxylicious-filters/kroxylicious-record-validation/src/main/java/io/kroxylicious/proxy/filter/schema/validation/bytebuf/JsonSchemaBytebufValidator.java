/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.validation.bytebuf;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;

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
    public Result validate(ByteBuffer buffer, int length, Record record, boolean isKey) {
        JsonValidationResult jsonValidationResult = jsonValidator.validateByArtifactReference(buffer.array());
        return jsonValidationResult.success() ? Result.VALID : new Result(false, jsonValidationResult.toString());
    }
}
