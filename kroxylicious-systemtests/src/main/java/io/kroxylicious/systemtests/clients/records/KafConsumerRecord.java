/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.records;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.header.internals.RecordHeaders;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The type Kaf consumer record.
 */
public class KafConsumerRecord extends BaseConsumerRecord {

    /**
     * Instantiates a new Kaf consumer record.
     *
     * @param headers the headers
     * @param timestamp the timestamp
     * @param timestampType the timestamp type
     * @param key the key
     * @param payload the payload
     * @param partition the partition
     * @param offset the offset
     * @param leaderEpoch the leader epoch
     */
    @JsonCreator
    public KafConsumerRecord(@JsonProperty("headers") List<Map<String, String>> headers, @JsonProperty("timestamp") String timestamp,
                             @JsonProperty("timestampType") String timestampType,
                             @JsonProperty("key") String key, @JsonProperty("payload") String payload, @JsonProperty("partition") int partition,
                             @JsonProperty("offset") long offset, @JsonProperty("leaderEpoch") int leaderEpoch) {
        this.recordHeaders = new RecordHeaders();
        if (headers != null) {
            headers.forEach(h -> h.forEach((headerKey, headerValue) -> recordHeaders.add(headerKey, headerValue.getBytes(StandardCharsets.UTF_8))));
        }
        this.timestamp = Instant.parse(timestamp).toEpochMilli();
        this.timestampType = timestampType;
        this.key = key;
        this.payload = payload;
        this.partition = partition;
        this.offset = offset;
        this.leaderEpoch = leaderEpoch;
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
