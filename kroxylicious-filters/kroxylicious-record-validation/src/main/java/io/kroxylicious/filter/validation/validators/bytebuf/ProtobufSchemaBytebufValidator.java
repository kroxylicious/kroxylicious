/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation.validators.bytebuf;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import io.apicurio.registry.resolver.DefaultSchemaResolver;
import io.apicurio.registry.resolver.SchemaResolver;
import io.apicurio.registry.resolver.strategy.ArtifactReference;
import io.apicurio.registry.utils.protobuf.schema.ProtobufSchema;
import io.apicurio.schema.validation.protobuf.ProtobufSchemaParser;
import io.apicurio.schema.validation.protobuf.ProtobufValidationResult;
import io.apicurio.schema.validation.protobuf.ProtobufValidator;

import io.kroxylicious.filter.validation.config.SchemaValidationConfig.WireFormatVersion;
import io.kroxylicious.filter.validation.validators.Result;

/**
 * Validates Kafka record values against a Protocol Buffers schema stored in Apicurio Registry.
 * <p>
 * Supports proto2 and proto3 syntax. The proto editions syntax (introduced in protobuf v27)
 * is not currently supported by Apicurio Registry.
 * </p>
 * <p>
 * Only the first message type defined in the {@code .proto} file is used for validation.
 * If the schema contains no message types, construction fails with an
 * {@link IllegalArgumentException}.
 * </p>
 * <p>
 * The Apicurio ProtobufSerde writes a delimited Ref message (containing the protobuf message
 * type name) before the actual protobuf payload in both header and body modes. This class
 * overrides {@link #skipExtraSerdeBytes(ByteBuffer)} to skip that Ref prefix before validation.
 * </p>
 */
class ProtobufSchemaBytebufValidator extends AbstractSchemaBytebufValidator {
    private final ProtobufValidator protobufValidator;
    private final Descriptors.Descriptor messageDescriptor;

    public ProtobufSchemaBytebufValidator(Map<String, Object> schemaResolverConfig, Long schemaId, WireFormatVersion wireFormatVersion) {
        super(schemaId, wireFormatVersion);
        this.protobufValidator = new ProtobufValidator(schemaResolverConfig, Optional.of(ArtifactReference.fromContentId(schemaId)));
        this.messageDescriptor = resolveProtobufDescriptor(schemaResolverConfig, schemaId);
    }

    @Override
    protected void skipExtraSerdeBytes(ByteBuffer buffer) {
        // The Apicurio ProtobufSerde writes a delimited Ref message (containing the protobuf
        // message type name) before the actual protobuf payload in both header and body modes.
        if (buffer.hasRemaining()) {
            int refSize = readVarint(buffer);
            for (int i = 0; i < refSize && buffer.hasRemaining(); i++) {
                buffer.get();
            }
        }
    }

    private static int readVarint(ByteBuffer buffer) {
        int value = 0;
        int shift = 0;
        byte b;
        do {
            if (!buffer.hasRemaining()) {
                throw new IllegalArgumentException("Truncated varint in Ref prefix");
            }
            if (shift >= 35) {
                throw new IllegalArgumentException("Varint too long in Ref prefix");
            }
            b = buffer.get();
            value |= (b & 0x7F) << shift;
            shift += 7;
        } while ((b & 0x80) != 0);
        return value;
    }

    @Override
    protected CompletionStage<Result> doValidate(ByteBuffer buffer) {
        try {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            DynamicMessage message = DynamicMessage.parseFrom(messageDescriptor, bytes);
            ProtobufValidationResult protobufValidationResult = protobufValidator.validateByArtifactReference(message);
            return protobufValidationResult.success() ? Result.VALID_RESULT_STAGE
                    : CompletableFuture.completedFuture(new Result(false, protobufValidationResult.toString()));
        }
        catch (InvalidProtocolBufferException e) {
            return CompletableFuture.completedFuture(new Result(false, "Failed to parse Protobuf message: " + e.getMessage()));
        }
    }

    private static Descriptors.Descriptor resolveProtobufDescriptor(Map<String, Object> schemaResolverConfig, Long schemaId) {
        try (SchemaResolver<ProtobufSchema, Message> resolver = new DefaultSchemaResolver<>()) {
            resolver.configure(schemaResolverConfig, new ProtobufSchemaParser<>());
            var schemaLookupResult = resolver.resolveSchemaByArtifactReference(ArtifactReference.fromContentId(schemaId));
            var protobufSchema = schemaLookupResult.getParsedSchema().getParsedSchema();
            var messageTypes = protobufSchema.getFileDescriptor().getMessageTypes();
            if (messageTypes.isEmpty()) {
                throw new IllegalArgumentException("Protobuf schema has no message types defined");
            }
            return messageTypes.get(0);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to resolve Protobuf schema", e);
        }
    }
}
