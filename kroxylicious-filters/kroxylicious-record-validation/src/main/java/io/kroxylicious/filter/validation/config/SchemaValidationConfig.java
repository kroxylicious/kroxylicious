/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.validation.config;

import java.net.URL;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for validating a component ByteBuffer of a {@link org.apache.kafka.common.record.Record} is valid using the schema in Apicurio Registry.
 * @param apicurioGlobalId globalId
 * @param apicurioRegistryUrl apicurio registry url
 */
public record SchemaValidationConfig(URL apicurioRegistryUrl, long apicurioId, WireFormatVersion wireFormatVersion) {

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
        @Deprecated(since = "0.12.0", forRemoval = true)
        V2,

        /**
         * Apicurio Registry v3 wire format using 4-byte content IDs (Confluent-compatible).
         * This is the recommended and default format.
         */
        V3
    }

    /**
     * Construct SchemaValidationConfig with explicit wire format version
     * @param apicurioRegistryUrl Apicurio Registry instance url
     * @param apicurioId schema identifier - interpreted as contentId for V3 (default) or globalId for V2
     * @param wireFormatVersion wire format version (defaults to V3 if null)
     */
    @JsonCreator
    public SchemaValidationConfig(@JsonProperty(value = "apicurioRegistryUrl", required = true) URL apicurioRegistryUrl,
                                  @JsonProperty(value = "apicurioId", required = true) long apicurioId,
                                  @JsonProperty(value = "wireFormatVersion", required = false) WireFormatVersion wireFormatVersion) {
        this.apicurioId = apicurioId;
        this.apicurioRegistryUrl = apicurioRegistryUrl;
        this.wireFormatVersion = wireFormatVersion != null ? wireFormatVersion : WireFormatVersion.V3;
    }

}
