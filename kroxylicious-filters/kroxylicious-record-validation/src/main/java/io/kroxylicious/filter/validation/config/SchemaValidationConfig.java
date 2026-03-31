/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation.config;

import java.net.URL;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.config.tls.Tls;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for validating a component ByteBuffer of a {@link org.apache.kafka.common.record.Record} is valid using the schema in Apicurio Registry.
 * @param apicurioRegistryUrl apicurio registry url
 * @param apicurioId schema identifier - interpreted as contentId for V3 (default) or globalId for V2
 * @param wireFormatVersion wire format version (defaults to V3 if null)
 * @param tls optional TLS configuration for connecting to a schema registry protected by TLS
 * @param schemaType the type of schema to validate against (defaults to JSON_SCHEMA if null)
 */
public record SchemaValidationConfig(URL apicurioRegistryUrl,
                                     long apicurioId,
                                     WireFormatVersion wireFormatVersion,
                                     @Nullable Tls tls,
                                     SchemaType schemaType) {

    /**
     * The type of schema stored in Apicurio Registry to validate against.
     */
    public enum SchemaType {
        /**
         * JSON Schema validation.
         */
        JSON_SCHEMA,

        /**
         * Apache Avro schema validation.
         */
        AVRO,

        /**
         * Protocol Buffers (Protobuf) schema validation.
         */
        PROTOBUF
    }

    /**
     * Wire format versions for Apicurio Registry schema identifiers.
     * <p>
     * V3 uses 4-byte content IDs (Confluent-compatible format) and is the default.
     * V2 uses 8-byte global IDs and is deprecated.
     * </p>
     */
    public enum WireFormatVersion {
        /**
         * Apicurio Registry v2 wire format using 8-byte global IDs.
         * @deprecated Use {@link #V3} instead for Confluent compatibility. This option will be removed in a future release.
         */
        @Deprecated(since = "0.19.0", forRemoval = true)
        V2,

        /**
         * Apicurio Registry v3 wire format using 4-byte content IDs (Confluent-compatible).
         * This is the recommended and default format.
         */
        V3
    }

    /**
     * Construct SchemaValidationConfig with explicit wire format version, TLS configuration, and schema type
     * @param apicurioRegistryUrl Apicurio Registry instance url
     * @param apicurioId schema identifier - interpreted as contentId for V3 (default) or globalId for V2
     * @param wireFormatVersion wire format version (defaults to V3 if null)
     * @param tls optional TLS configuration for connecting to a schema registry protected by TLS with a custom trust store
     * @param schemaType the type of schema to validate against (defaults to JSON_SCHEMA if null)
     */
    @JsonCreator
    public SchemaValidationConfig(@JsonProperty(value = "apicurioRegistryUrl", required = true) URL apicurioRegistryUrl,
                                  @JsonProperty(value = "apicurioId", required = true) long apicurioId,
                                  @Nullable @JsonProperty(value = "wireFormatVersion") WireFormatVersion wireFormatVersion,
                                  @JsonProperty(value = "tls") @Nullable Tls tls,
                                  @Nullable @JsonProperty(value = "schemaType") SchemaType schemaType) {
        this.apicurioId = apicurioId;
        this.apicurioRegistryUrl = apicurioRegistryUrl;
        this.wireFormatVersion = wireFormatVersion != null ? wireFormatVersion : WireFormatVersion.V3;
        this.tls = tls;
        this.schemaType = schemaType != null ? schemaType : SchemaType.JSON_SCHEMA;
    }

}
