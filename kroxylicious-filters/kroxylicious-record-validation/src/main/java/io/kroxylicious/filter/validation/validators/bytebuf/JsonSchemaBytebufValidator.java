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

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.Record;

import io.apicurio.registry.resolver.strategy.ArtifactReference;
import io.apicurio.registry.serde.BaseSerde;
import io.apicurio.registry.serde.Default4ByteIdHandler;
import io.apicurio.registry.serde.IdHandler;
import io.apicurio.registry.serde.Legacy8ByteIdHandler;
import io.apicurio.registry.serde.headers.DefaultHeadersHandler;
import io.apicurio.registry.serde.headers.HeadersHandler;
import io.apicurio.schema.validation.json.JsonValidationResult;
import io.apicurio.schema.validation.json.JsonValidator;

import io.kroxylicious.filter.validation.config.SchemaValidationConfig.WireFormatVersion;
import io.kroxylicious.filter.validation.validators.Result;

public class JsonSchemaBytebufValidator implements BytebufValidator {
    private final JsonValidator jsonValidator;
    private final Long schemaId;
    private final WireFormatVersion wireFormatVersion;
    private final HeadersHandler keyHeaderHandler;
    private final HeadersHandler valueHeaderHandler;
    private final IdHandler keyIdHandler;
    private final IdHandler valueIdHandler;

    public JsonSchemaBytebufValidator(Map<String, Object> schemaResolverConfig, Long schemaId, WireFormatVersion wireFormatVersion) {
        this.schemaId = schemaId;
        this.wireFormatVersion = wireFormatVersion;
        this.jsonValidator = new JsonValidator(schemaResolverConfig, Optional.of(ArtifactReference.fromContentId(schemaId)));
        this.keyHeaderHandler = buildHeaderHandler(true);
        this.keyIdHandler = buildIdHandler(true, wireFormatVersion);

        this.valueHeaderHandler = buildHeaderHandler(false);
        this.valueIdHandler = buildIdHandler(false, wireFormatVersion);
    }

    @Override
    public CompletionStage<Result> validate(ByteBuffer buffer, Record record, boolean isKey) {
        try {
            // If the record includes a schema id, validate that it is consistent with the expected schemaId.
            Optional<Long> extractedSchemaId = extractSchemaIdFromRecord(buffer, record, isKey);
            if (extractedSchemaId.filter(e -> !e.equals(schemaId)).isPresent()) {
                return CompletableFuture
                        .completedStage(new Result(false, "Unexpected schema id in record (%d), expecting %d".formatted(extractedSchemaId.get(), schemaId)));
            }

            JsonValidationResult jsonValidationResult = jsonValidator.validateByArtifactReference(buffer);
            return jsonValidationResult.success() ? Result.VALID_RESULT_STAGE
                    : CompletableFuture.completedFuture(new Result(false, jsonValidationResult.toString()));
        }
        catch (RuntimeException ex) {
            return CompletableFuture.completedStage(new Result(false, ex.getMessage()));
        }
    }

    @SuppressWarnings("removal")
    private Optional<Long> extractSchemaIdFromRecord(ByteBuffer buffer, Record kafkaRecord, boolean isKey) {
        if (kafkaRecord.headers().length > 0) {
            var recordHeaders = new RecordHeaders(kafkaRecord.headers());
            var headerHandler = isKey ? keyHeaderHandler : valueHeaderHandler;
            var ref = headerHandler.readHeaders(recordHeaders);

            // V2 uses globalId in headers, V3 uses contentId
            Long id = switch (wireFormatVersion) {
                case V2 -> ref.getGlobalId();
                case V3 -> ref.getContentId();
            };
            if (id != null) {
                return Optional.of(id);
            }
        }

        var idHandler = isKey ? keyIdHandler : valueIdHandler;
        var minBytes = 1 + idHandler.idSize();
        if (buffer.remaining() > minBytes && buffer.get(buffer.position()) == BaseSerde.MAGIC_BYTE) {
            buffer.get(); // ignore magic
            // V2 uses globalId in wire format, V3 uses contentId
            var ref = idHandler.readId(buffer);
            Long id = switch (wireFormatVersion) {
                case V2 -> ref.getGlobalId();
                case V3 -> ref.getContentId();
            };
            return Optional.ofNullable(id);
        }
        return Optional.empty();
    }

    private static DefaultHeadersHandler buildHeaderHandler(boolean isKey) {
        var handler = new DefaultHeadersHandler();
        handler.configure(Map.of(), isKey);
        return handler;
    }

    @SuppressWarnings("removal")
    private static IdHandler buildIdHandler(boolean isKey, WireFormatVersion wireFormatVersion) {
        IdHandler handler = switch (wireFormatVersion) {
            case V2 -> new Legacy8ByteIdHandler();
            case V3 -> new Default4ByteIdHandler();
        };
        handler.configure(Map.of(), isKey);
        return handler;
    }
}
