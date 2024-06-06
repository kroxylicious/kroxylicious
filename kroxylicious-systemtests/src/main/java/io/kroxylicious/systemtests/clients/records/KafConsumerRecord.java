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
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.systemtests.utils.KafkaUtils;

/**
 * The type Kaf consumer record.
 */
public class KafConsumerRecord extends BaseConsumerRecord {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafConsumerRecord.class);

    private String timestamp;
    private String timestampType;
    private List<Map<String, String>> headers;

    /**
     * Sets timestamp.
     *
     * @param timestamp the timestamp
     */
    public void setTimestamp(String timestamp) {
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
    public void setHeaders(List<Map<String, String>> headers) {
        this.headers = headers;
    }

    /**
     * Parse from json string.
     *
     * @param response the response
     * @return the kaf consumer record
     */
    public static KafConsumerRecord parseFromJsonString(String response) {
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            return objectMapper.readValue(response, KafConsumerRecord.class);
        }
        catch (JsonProcessingException e) {
            LOGGER.atError().setMessage("Error: {}").addArgument(e.getMessage()).log();
            return null;
        }
    }

    /**
     * To consumer record.
     *
     * @return the consumer record
     */
    public ConsumerRecord<String, String> toConsumerRecord() {
        Headers recordHeaders = new RecordHeaders();
        if (this.headers != null) {
            this.headers.forEach(h -> h.forEach((headerKey, headerValue) -> recordHeaders.add(headerKey, headerValue.getBytes(StandardCharsets.UTF_8))));
        }
        return new ConsumerRecord<>(
                this.topic,
                this.partition,
                this.offset,
                Instant.parse(this.timestamp).toEpochMilli(),
                KafkaUtils.getTimestampType(this.timestampType),
                -1,
                -1,
                (String) this.key,
                String.valueOf(this.payload),
                recordHeaders,
                Optional.ofNullable(this.leaderEpoch));
    }
}
