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

import io.apicurio.registry.serde.BaseSerde;
import io.apicurio.registry.serde.Default4ByteIdHandler;
import io.apicurio.registry.serde.IdHandler;
import io.apicurio.registry.serde.Legacy8ByteIdHandler;
import io.apicurio.registry.serde.kafka.headers.DefaultHeadersHandler;
import io.apicurio.registry.serde.kafka.headers.HeadersHandler;

import io.kroxylicious.filter.validation.config.SchemaValidationConfig.WireFormatVersion;
import io.kroxylicious.filter.validation.validators.Result;

/**
 * Base class for schema-based record validators that validate Kafka record values (or keys)
 * against a schema stored in Apicurio Registry.
 * <p>
 * This class handles the Apicurio serde wire format, extracting the schema identifier from
 * the record (either from Kafka headers or from a magic-byte prefix in the record body) and
 * verifying it matches the expected schema. After stripping the serde envelope, it delegates
 * actual schema validation to the subclass via {@link #doValidate(ByteBuffer)}.
 * </p>
 * <p>
 * Subclasses must implement {@link #doValidate(ByteBuffer)} to perform schema-specific
 * validation (e.g. JSON Schema, Avro, Protobuf). Subclasses may also override
 * {@link #skipExtraSerdeBytes(ByteBuffer)} if the serde writes additional framing bytes
 * between the schema identifier and the payload (e.g. the Protobuf serde writes a
 * delimited Ref message).
 * </p>
 */
abstract class AbstractSchemaBytebufValidator implements BytebufValidator {
    private final Long schemaId;
    private final WireFormatVersion wireFormatVersion;
    private final HeadersHandler keyHeaderHandler;
    private final HeadersHandler valueHeaderHandler;
    private final IdHandler keyIdHandler;
    private final IdHandler valueIdHandler;

    protected AbstractSchemaBytebufValidator(Long schemaId, WireFormatVersion wireFormatVersion) {
        this.schemaId = schemaId;
        this.wireFormatVersion = wireFormatVersion;
        this.keyHeaderHandler = buildHeaderHandler(true);
        this.keyIdHandler = buildIdHandler(true, wireFormatVersion);
        this.valueHeaderHandler = buildHeaderHandler(false);
        this.valueIdHandler = buildIdHandler(false, wireFormatVersion);
    }

    @Override
    public CompletionStage<Result> validate(ByteBuffer buffer, Record record, boolean isKey) {
        try {
            Optional<Long> extractedSchemaId = extractSchemaIdFromRecord(buffer, record, isKey);
            if (extractedSchemaId.filter(e -> !e.equals(schemaId)).isPresent()) {
                return CompletableFuture
                        .completedStage(new Result(false,
                                "Unexpected schema id in record (%d), expecting %d".formatted(extractedSchemaId.get(), schemaId)));
            }

            if (extractedSchemaId.isPresent()) {
                skipExtraSerdeBytes(buffer);
            }

            return doValidate(buffer);
        }
        catch (RuntimeException ex) {
            return CompletableFuture.completedStage(new Result(false, ex.getMessage()));
        }
    }

    protected abstract CompletionStage<Result> doValidate(ByteBuffer buffer);

    /**
     * Called when schema ID was found in the record (either in headers or body prefix).
     * The body may contain serde-specific prefix bytes that need to be skipped before the
     * actual schema data. For example, the Protobuf serde writes a Ref message before
     * the protobuf payload in both header and body modes.
     * Default implementation does nothing (JSON and Avro serdes have no extra prefix).
     */
    protected void skipExtraSerdeBytes(ByteBuffer buffer) {
        // default: no extra bytes to skip
    }

    @SuppressWarnings("removal")
    private Optional<Long> extractSchemaIdFromRecord(ByteBuffer buffer, Record kafkaRecord, boolean isKey) {
        // Try headers first
        var headerId = extractSchemaIdFromHeaders(kafkaRecord, isKey);
        if (headerId.isPresent()) {
            return headerId;
        }

        // Fall back to body prefix
        var idHandler = isKey ? keyIdHandler : valueIdHandler;
        var minBytes = 1 + idHandler.idSize();
        if (buffer.remaining() > minBytes && buffer.get(buffer.position()) == BaseSerde.MAGIC_BYTE) {
            buffer.get(); // ignore magic
            var ref = idHandler.readId(buffer);
            Long id = switch (wireFormatVersion) {
                case V2 -> ref.getGlobalId();
                case V3 -> ref.getContentId();
            };
            return Optional.ofNullable(id);
        }
        return Optional.empty();
    }

    @SuppressWarnings("removal")
    private Optional<Long> extractSchemaIdFromHeaders(Record kafkaRecord, boolean isKey) {
        if (kafkaRecord.headers().length > 0) {
            var recordHeaders = new RecordHeaders(kafkaRecord.headers());
            var headerHandler = isKey ? keyHeaderHandler : valueHeaderHandler;
            var ref = headerHandler.readHeaders(recordHeaders);

            Long id = switch (wireFormatVersion) {
                case V2 -> ref.getGlobalId();
                case V3 -> ref.getContentId();
            };
            if (id != null) {
                return Optional.of(id);
            }
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
