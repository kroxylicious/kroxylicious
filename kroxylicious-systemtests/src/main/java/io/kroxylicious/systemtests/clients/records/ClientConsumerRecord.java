/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.records;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import io.kroxylicious.systemtests.utils.KafkaUtils;

/**
 * The type Client consumer record.
 */
public class ClientConsumerRecord extends BaseConsumerRecord {
    private long timestamp;
    private String timestampType;
    private List<Entry<String, String>> headers;

    /**
     * Sets timestamp.
     *
     * @param timestamp the timestamp
     */
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Sets timestamp type.
     *
     * @param timestampType the timestamp type
     */
    public void setTimestampType(String timestampType) {
        this.timestampType = timestampType;
    }

    /**
     * Sets headers.
     *
     * @param headers the headers
     */
    public void setHeaders(List<Entry<String, String>> headers) {
        this.headers = headers;
    }

    /**
     * To consumer record.
     *
     * @return the consumer record
     */
    public ConsumerRecord<String, String> toConsumerRecord() {
        Headers recordHeaders = new RecordHeaders();
        if (this.headers != null) {
            this.headers.forEach(h -> recordHeaders.add(h.getKey(), h.getValue().getBytes(StandardCharsets.UTF_8)));
        }
        return new ConsumerRecord<>(
                this.topic,
                this.partition,
                this.offset,
                this.timestamp,
                KafkaUtils.getTimestampType(this.timestampType),
                -1,
                -1,
                (String) this.key,
                String.valueOf(this.payload),
                recordHeaders,
                Optional.ofNullable(this.leaderEpoch));
    }
}
