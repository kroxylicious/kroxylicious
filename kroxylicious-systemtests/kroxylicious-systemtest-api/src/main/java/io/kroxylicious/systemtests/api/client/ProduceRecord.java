/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.client;

import java.util.Map;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A record a test wants to produce.
 *
 * @param key record key, or {@code null} for no key
 * @param value record value
 * @param headers record headers; requires {@link ClientCapability#RECORD_HEADERS} when non-empty
 */
public record ProduceRecord(@Nullable String key, @NonNull String value, @NonNull Map<String, String> headers) {

    public ProduceRecord {
        Objects.requireNonNull(value, "value must not be null");
        headers = Map.copyOf(Objects.requireNonNull(headers, "headers must not be null"));
    }

    /**
     * Creates a record with no key and no headers.
     *
     * @param value record value
     * @return the record
     */
    @NonNull
    public static ProduceRecord of(@NonNull String value) {
        return new ProduceRecord(null, value, Map.of());
    }

    /**
     * Creates a record with a key and no headers.
     *
     * @param key record key, or {@code null} for no key
     * @param value record value
     * @return the record
     */
    @NonNull
    public static ProduceRecord of(@Nullable String key, @NonNull String value) {
        return new ProduceRecord(key, value, Map.of());
    }
}
