/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.records;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.common.header.internals.RecordHeaders;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The type Kcat consumer record.
 */
public class KcatConsumerRecord extends BaseConsumerRecord {

    /**
     * Instantiates a new Kcat consumer record.
     *
     * @param headers the headers
     * @param timestamp the timestamp
     * @param timestampType the timestamp type
     * @param topic the topic
     * @param broker the broker
     * @param key the key
     * @param payload the payload
     * @param partition the partition
     * @param offset the offset
     * @param leaderEpoch the leader epoch
     */
    @JsonCreator
    public KcatConsumerRecord(@JsonProperty("headers") List<String> headers,
                              @JsonProperty("ts") long timestamp,
                              @JsonProperty("tstype") String timestampType,
                              @JsonProperty("topic") String topic,
                              @JsonProperty("broker") int broker,
                              @JsonProperty("key") String key,
                              @JsonProperty("payload") String payload,
                              @JsonProperty("partition") int partition,
                              @JsonProperty("offset") long offset,
                              @JsonProperty("leaderEpoch") int leaderEpoch) {
        this.recordHeaders = new RecordHeaders();
        if (headers != null) {
            if (headers.size() % 2 != 0) {
                throw new IllegalArgumentException("Invalid headers size. It must be even, not odd!");
            }
            for (int i = 0; i < headers.size(); i += 2) {
                recordHeaders.add(headers.get(i), Optional.ofNullable(headers.get(i + 1)).orElse("").getBytes(StandardCharsets.UTF_8));
            }
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
