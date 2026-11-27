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
 * A record a {@link KafkaClient} consumed, independent of the driver that consumed it.
 *
 * @param topic topic the record was consumed from
 * @param partition partition the record was consumed from
 * @param offset offset of the record within its partition
 * @param key record key, or {@code null} if the record has no key
 * @param value record value, or {@code null} if the record has no value
 * @param headers record headers (empty if the driver does not report them)
 */
public record ConsumedRecord(@NonNull String topic, int partition, long offset, @Nullable String key, @Nullable String value,
                             @NonNull Map<String, String> headers) {

    public ConsumedRecord {
        Objects.requireNonNull(topic, "topic must not be null");
        headers = Map.copyOf(Objects.requireNonNull(headers, "headers must not be null"));
    }
}
