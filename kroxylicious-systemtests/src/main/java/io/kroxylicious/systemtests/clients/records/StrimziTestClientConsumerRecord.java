/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.records;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The type Strimzi Test Client consumer record.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StrimziTestClientConsumerRecord extends ConsumerRecord {

    /**
     * Instantiates a new Client consumer record.
     *
     * @param headers the headers
     * @param topic the topic
     * @param key the key
     * @param payload the payload
     * @param partition the partition
     * @param offset the offset
     */
    @JsonCreator
    public StrimziTestClientConsumerRecord(@JsonProperty("headers") List<Entry<String, String>> headers,
                                           @JsonProperty("topic") String topic,
                                           @JsonProperty("key") String key,
                                           @JsonProperty("payload") String payload,
                                           @JsonProperty("partition") int partition,
                                           @JsonProperty("offset") long offset) {
        this.recordHeaders = new HashMap<>();
        if (headers != null) {
            headers.forEach(h -> recordHeaders.put(h.getKey(), h.getValue()));
        }
        this.topic = topic;
        this.key = key;
        this.payload = payload;
        this.partition = partition;
        this.offset = offset;
    }
}
