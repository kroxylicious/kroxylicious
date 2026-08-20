/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients;

/**
 * Compression type accepted by the Kafka {@code compression.type} producer/topic config.
 */
public enum CompressionType {
    NONE("none"),
    GZIP("gzip"),
    SNAPPY("snappy"),
    LZ4("lz4"),
    ZSTD("zstd");

    /** the on-wire config value, e.g. "gzip" */
    public final String name;

    CompressionType(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
