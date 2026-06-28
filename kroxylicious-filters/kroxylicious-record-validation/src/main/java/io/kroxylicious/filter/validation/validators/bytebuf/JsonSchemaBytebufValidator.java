/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation.validators.bytebuf;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.apicurio.registry.resolver.strategy.ArtifactReference;
import io.apicurio.schema.validation.json.JsonValidationResult;
import io.apicurio.schema.validation.json.JsonValidator;

import io.kroxylicious.filter.validation.config.SchemaValidationConfig.WireFormatVersion;
import io.kroxylicious.filter.validation.validators.Result;

/**
 * Validates Kafka record keys and values against a JSON Schema stored in Apicurio Registry.
 * <p>
 * Supports JSON Schema drafts as accepted by Apicurio Registry (Draft-04 through Draft 2020-12).
 * The JSON payload is validated after stripping any Apicurio serde envelope (schema ID in
 * headers or magic-byte body prefix).
 * </p>
 */
class JsonSchemaBytebufValidator extends AbstractSchemaBytebufValidator {
    private final JsonValidator jsonValidator;

    JsonSchemaBytebufValidator(Map<String, Object> schemaResolverConfig, Long schemaId, WireFormatVersion wireFormatVersion) {
        super(schemaId, wireFormatVersion);
        this.jsonValidator = new JsonValidator(schemaResolverConfig, Optional.of(ArtifactReference.fromContentId(schemaId)));
    }

    @Override
    protected CompletionStage<Result> doValidate(ByteBuffer buffer) {
        JsonValidationResult jsonValidationResult = jsonValidator.validateByArtifactReference(buffer);
        return jsonValidationResult.success() ? Result.VALID_RESULT_STAGE
                : CompletableFuture.completedFuture(new Result(false, jsonValidationResult.toString()));
    }
}
