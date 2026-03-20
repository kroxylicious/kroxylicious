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

public class ProtobufSchemaBytebufValidator extends AbstractSchemaBytebufValidator {
    private final ProtobufValidator protobufValidator;
    private final Descriptors.Descriptor messageDescriptor;

    public ProtobufSchemaBytebufValidator(Map<String, Object> schemaResolverConfig, Long schemaId, WireFormatVersion wireFormatVersion) {
        super(schemaId, wireFormatVersion);
        this.protobufValidator = new ProtobufValidator(schemaResolverConfig, Optional.of(ArtifactReference.fromContentId(schemaId)));
        this.messageDescriptor = resolveProtobufDescriptor(schemaResolverConfig, schemaId);
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
            return protobufSchema.getFileDescriptor().getMessageTypes().get(0);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to resolve Protobuf schema", e);
        }
    }
}
