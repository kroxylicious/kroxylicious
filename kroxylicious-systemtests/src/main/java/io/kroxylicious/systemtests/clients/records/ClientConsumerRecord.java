/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.records;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map.Entry;

import org.apache.kafka.common.header.internals.RecordHeaders;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The type Client consumer record.
 */
public class ClientConsumerRecord extends BaseConsumerRecord {

    /**
     * Instantiates a new Client consumer record.
     *
     * @param headers the headers
     * @param timestamp the timestamp
     * @param timestampType the timestamp type
     * @param topic the topic
     * @param key the key
     * @param payload the payload
     * @param partition the partition
     * @param offset the offset
     * @param leaderEpoch the leader epoch
     */
    @JsonCreator
    public ClientConsumerRecord(@JsonProperty("headers") List<Entry<String, String>> headers, @JsonProperty("timestamp") long timestamp,
                             @JsonProperty("timestampType") String timestampType, @JsonProperty("topic") String topic,
                             @JsonProperty("key") String key, @JsonProperty("payload") String payload, @JsonProperty("partition") int partition,
                             @JsonProperty("offset") long offset, @JsonProperty("leaderEpoch") int leaderEpoch) {
        this.recordHeaders = new RecordHeaders();
        if (headers != null) {
            headers.forEach(h -> recordHeaders.add(h.getKey(), h.getValue().getBytes(StandardCharsets.UTF_8)));
        }
        this.timestamp = timestamp;
        this.timestampType = timestampType;
        this.topic = topic;
        this.key = key;
        this.payload = payload;
        this.partition = partition;
        this.offset = offset;
        this.leaderEpoch = leaderEpoch;
    }
}
