/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.records;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The type Kcat consumer record.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class KcatConsumerRecord extends ConsumerRecord {

    /**
     * Instantiates a new Kcat consumer record.
     *
     * @param headers the headers
     * @param topic the topic
     * @param key the key
     * @param payload the payload
     * @param partition the partition
     * @param offset the offset
     */
    @JsonCreator
    public KcatConsumerRecord(@JsonProperty("headers") List<String> headers,
                              @JsonProperty("topic") String topic,
                              @JsonProperty("key") String key,
                              @JsonProperty("payload") String payload,
                              @JsonProperty("partition") int partition,
                              @JsonProperty("offset") long offset) {
        super(topic, key, payload, partition, offset);
        this.recordHeaders = new HashMap<>();
        if (headers != null) {
            int headersSize = headers.size();
            if (headersSize % 2 != 0) {
                throw new IllegalArgumentException("Invalid headers size. It must be even, not odd!");
            }
            for (int i = 0; i < headersSize; i += 2) {
                recordHeaders.put(headers.get(i), Optional.ofNullable(headers.get(i + 1)).orElse(""));
            }
        }
    }
}
