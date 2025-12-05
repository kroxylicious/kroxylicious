/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.config;

import java.net.URL;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for validating a component ByteBuffer of a {@link org.apache.kafka.common.record.Record} is valid using the schema in Apicurio Registry.
 */
public record SchemaValidationConfig(URL apicurioRegistryUrl, long apicurioContentId, WireFormatVersion wireFormatVersion) {

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
     * @param apicurioContentId apicurio registry content identifier to be used for schema validation
     * @param wireFormatVersion wire format version (defaults to V3 if null)
     */
    @JsonCreator
    public SchemaValidationConfig(@JsonProperty(value = "apicurioRegistryUrl", required = true) URL apicurioRegistryUrl,
                                  @JsonProperty(value = "apicurioContentId", required = true) long apicurioContentId,
                                  @JsonProperty(value = "wireFormatVersion", required = false) WireFormatVersion wireFormatVersion) {
        this.apicurioContentId = apicurioContentId;
        this.apicurioRegistryUrl = apicurioRegistryUrl;
        this.wireFormatVersion = wireFormatVersion != null ? wireFormatVersion : WireFormatVersion.V3;
    }

    @Override
    public String toString() {
        return "SchemaValidationConfig{" +
                "apicurioContentId=" + apicurioContentId +
                ", apicurioRegistryUrl='" + apicurioRegistryUrl + '\'' +
                ", wireFormatVersion=" + wireFormatVersion +
                '}';
    }

}
