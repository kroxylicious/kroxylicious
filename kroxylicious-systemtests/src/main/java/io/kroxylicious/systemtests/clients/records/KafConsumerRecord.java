/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.records;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The type Kaf consumer record.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafConsumerRecord extends ConsumerRecord {

    /**
     * Instantiates a new Kaf consumer record.
     *
     * @param headers the headers
     * @param key the key
     * @param payload the payload
     * @param partition the partition
     * @param offset the offset
     */
    @JsonCreator
    public KafConsumerRecord(@JsonProperty("headers") List<Map<String, String>> headers,
                             @JsonProperty("key") String key,
                             @JsonProperty("payload") String payload,
                             @JsonProperty("partition") int partition,
                             @JsonProperty("offset") long offset) {
        this.recordHeaders = new HashMap<>();
        if (headers != null) {
            headers.forEach(h -> recordHeaders.put(new String(Base64.getDecoder().decode(h.get("Key"))), new String(Base64.getDecoder().decode(h.get("Value")))));
        }
        this.key = key;
        this.payload = payload;
        this.partition = partition;
        this.offset = offset;
    }

    /**
     * Sets topic.
     *
     * @param topic the topic
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }
}
