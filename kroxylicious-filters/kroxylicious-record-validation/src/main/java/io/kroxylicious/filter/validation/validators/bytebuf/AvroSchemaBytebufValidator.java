/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation.validators.bytebuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;

import io.apicurio.registry.resolver.DefaultSchemaResolver;
import io.apicurio.registry.resolver.SchemaResolver;
import io.apicurio.registry.resolver.strategy.ArtifactReference;
import io.apicurio.schema.validation.avro.AvroSchemaParser;
import io.apicurio.schema.validation.avro.AvroValidationResult;
import io.apicurio.schema.validation.avro.AvroValidator;

import io.kroxylicious.filter.validation.config.SchemaValidationConfig.WireFormatVersion;
import io.kroxylicious.filter.validation.validators.Result;

public class AvroSchemaBytebufValidator extends AbstractSchemaBytebufValidator {
    private final AvroValidator avroValidator;
    private final Schema avroSchema;

    public AvroSchemaBytebufValidator(Map<String, Object> schemaResolverConfig, Long schemaId, WireFormatVersion wireFormatVersion) {
        super(schemaId, wireFormatVersion);
        this.avroValidator = new AvroValidator(schemaResolverConfig, Optional.of(ArtifactReference.fromContentId(schemaId)));
        this.avroSchema = resolveAvroSchema(schemaResolverConfig, schemaId);
    }

    @Override
    protected CompletionStage<Result> doValidate(ByteBuffer buffer) {
        try {
            GenericRecord record = deserialize(buffer);
            AvroValidationResult avroValidationResult = avroValidator.validateByArtifactReference(record);
            return avroValidationResult.success() ? Result.VALID_RESULT_STAGE
                    : CompletableFuture.completedFuture(new Result(false, avroValidationResult.toString()));
        }
        catch (IOException e) {
            return CompletableFuture.completedFuture(new Result(false, "Failed to deserialize Avro record: " + e.getMessage()));
        }
    }

    private GenericRecord deserialize(ByteBuffer buffer) throws IOException {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        var decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        var reader = new GenericDatumReader<GenericRecord>(avroSchema);
        return reader.read(null, decoder);
    }

    private static Schema resolveAvroSchema(Map<String, Object> schemaResolverConfig, Long schemaId) {
        try (SchemaResolver<Schema, GenericRecord> resolver = new DefaultSchemaResolver<>()) {
            resolver.configure(schemaResolverConfig, new AvroSchemaParser());
            var schemaLookupResult = resolver.resolveSchemaByArtifactReference(ArtifactReference.fromContentId(schemaId));
            return schemaLookupResult.getParsedSchema().getParsedSchema();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to resolve Avro schema", e);
        }
    }
}
