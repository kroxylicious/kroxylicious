/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.validators.bytebuf;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.record.Record;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.proxy.filter.validation.validators.Result;

public class KarapaceSchemaBytebufValidator implements BytebufValidator {
    private final HttpClient httpClient;
    private final String registryUrl;
    private final int schemaId;
    private final byte magicByte = 0x0;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @SuppressWarnings({ "checkstyle:RegexpSinglelineJava" })
    public KarapaceSchemaBytebufValidator(Map<String, Object> config, int schemaId) {
        this.registryUrl = config.get("karapace.registry.url").toString();
        this.schemaId = schemaId;
        this.httpClient = HttpClient.newHttpClient();
    }

    @Override
    public CompletionStage<Result> validate(ByteBuffer buffer, Record record, boolean isKey) {
        try {
            // If the record includes a schema id, validate that it is consistent with the expected schemaId
            Optional<Integer> extractedSchemaId = extractSchemaIdFromRecord(buffer, record, isKey);
            if (extractedSchemaId.isEmpty()) {
                return CompletableFuture
                        .completedStage(new Result(false, "No schema id in record (%d), expecting %d".formatted(extractedSchemaId.get(), schemaId)));
            }
            else if (extractedSchemaId.filter(e -> e != schemaId).isPresent()) {
                return CompletableFuture
                        .completedStage(new Result(false, "Unexpected schema id in record (%d), expecting %d".formatted(extractedSchemaId.get(), schemaId)));
            }
            return CompletableFuture.completedStage(new Result(true, "Validated successfully"));
        }
        catch (RuntimeException ex) {
            return CompletableFuture.completedStage(new Result(false, ex.getMessage()));
        }
    }

    private String getSchemaFromRegistry() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(registryUrl + "/schemas/ids/" + schemaId))
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                JsonNode jsonNode = objectMapper.readTree(response.body());
                return jsonNode.get("schema").asText();
            }
            return null;
        }
        catch (IOException | InterruptedException e) {
            throw new RuntimeException("Failed to get schema from registry: " + e.getMessage(), e);
        }
    }

    private Optional<Integer> extractSchemaIdFromRecord(ByteBuffer buffer, Record record, boolean isKey) {
        // Check if schema ID is present in headers
        if (record.headers().length > 0) {
            for (org.apache.kafka.common.header.Header header : record.headers()) {
                if ("schemaId".equals(header.key()) && header.value() != null && header.value().length >= 4) {
                    ByteBuffer headerBuffer = ByteBuffer.wrap(header.value());
                    return Optional.of(headerBuffer.getInt());
                }
            }
        }

        // Check if schema ID is present in message body (Confluent wire format)
        if (buffer != null && buffer.remaining() >= 5) {
            ByteBuffer buf = buffer.duplicate();
            byte magic = buf.get();
            if (magic == 0) {
                int schemaId = buf.getInt();
                return Optional.of(schemaId);
            }
        }

        return Optional.empty();
    }

    private byte[] extractPayload(ByteBuffer buffer) {
        if (buffer == null || buffer.remaining() < 5) { // Need at least magic byte + 4 bytes schema ID
            return null;
        }

        ByteBuffer buf = buffer.duplicate();
        byte magic = buf.get();
        if (magic != magicByte) {
            return null;
        }

        // Skip schema ID (4 bytes)
        buf.getInt();

        // Extract the remaining bytes as payload
        byte[] payload = new byte[buf.remaining()];
        buf.get(payload);
        return payload;
    }
}
