/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.bytebuf;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.Record;

import io.apicurio.registry.resolver.strategy.ArtifactReference;
import io.apicurio.registry.serde.AbstractKafkaSerDe;
import io.apicurio.registry.serde.DefaultIdHandler;
import io.apicurio.registry.serde.IdHandler;
import io.apicurio.registry.serde.headers.DefaultHeadersHandler;
import io.apicurio.registry.serde.headers.HeadersHandler;
import io.apicurio.schema.validation.json.JsonValidationResult;
import io.apicurio.schema.validation.json.JsonValidator;

import io.kroxylicious.proxy.filter.validation.validators.Result;

public class JsonSchemaBytebufValidator implements BytebufValidator {
    private final JsonValidator jsonValidator;
    private final Long globalId;
    private final HeadersHandler keyHeaderHandler;
    private final HeadersHandler valueHeaderHandler;
    private final IdHandler keyIdHandler;
    private final IdHandler valueIdHandler;

    public JsonSchemaBytebufValidator(Map<String, Object> schemaResolverConfig, Long globalId) {
        this.globalId = globalId;
        this.jsonValidator = new JsonValidator(schemaResolverConfig, Optional.of(ArtifactReference.fromGlobalId(globalId)));
        this.keyHeaderHandler = buildHeaderHandler(true);
        this.keyIdHandler = buildIdHandler(true);

        this.valueHeaderHandler = buildHeaderHandler(false);
        this.valueIdHandler = buildIdHandler(false);
    }

    @Override
    public CompletionStage<Result> validate(ByteBuffer buffer, Record record, boolean isKey) {
        try {
            // If the record includes a schema id, validate that it is consistent with the expected globalId.
            Optional<Long> extractedGlobalId = extractGlobalIdFromRecord(buffer, record, isKey);
            if (extractedGlobalId.filter(e -> !e.equals(globalId)).isPresent()) {
                return CompletableFuture
                        .completedStage(new Result(false, "Unexpected schema id in record (%d), expecting %d".formatted(extractedGlobalId.get(), globalId)));
            }

            JsonValidationResult jsonValidationResult = jsonValidator.validateByArtifactReference(buffer);
            return jsonValidationResult.success() ? Result.VALID_RESULT_STAGE
                    : CompletableFuture.completedFuture(new Result(false, jsonValidationResult.toString()));
        }
        catch (RuntimeException ex) {
            return CompletableFuture.completedStage(new Result(false, ex.getMessage()));
        }
    }

    private Optional<Long> extractGlobalIdFromRecord(ByteBuffer buffer, Record kafkaRecord, boolean isKey) {
        if (kafkaRecord.headers().length > 0) {
            var recordHeaders = new RecordHeaders(kafkaRecord.headers());
            var headerHandler = isKey ? keyHeaderHandler : valueHeaderHandler;
            var ref = headerHandler.readHeaders(recordHeaders);

            if (ref.getGlobalId() != null) {
                return Optional.of(ref.getGlobalId());
            }
        }

        var idHandler = isKey ? keyIdHandler : valueIdHandler;
        var minBytes = 1 + idHandler.idSize();
        if (buffer.remaining() > minBytes && buffer.get(buffer.position()) == AbstractKafkaSerDe.MAGIC_BYTE) {
            buffer.get(); // ignore magic
            return Optional.of(idHandler.readId(buffer).getGlobalId());
        }
        return Optional.empty();
    }

    private static DefaultHeadersHandler buildHeaderHandler(boolean isKey) {
        var handler = new DefaultHeadersHandler();
        handler.configure(Map.of(), isKey);
        return handler;
    }

    private static DefaultIdHandler buildIdHandler(boolean isKey) {
        var handler = new DefaultIdHandler();
        handler.configure(Map.of(), isKey);
        return handler;
    }
}
