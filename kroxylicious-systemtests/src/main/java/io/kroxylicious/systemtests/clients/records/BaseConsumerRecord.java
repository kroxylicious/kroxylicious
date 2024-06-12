/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.records;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kroxylicious.systemtests.utils.KafkaUtils;

public class BaseConsumerRecord {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseConsumerRecord.class);

    protected String topic;
    protected long timestamp;
    protected String timestampType;
    protected String key;
    protected Object payload;
    protected int partition;
    protected long offset;
    protected Integer leaderEpoch;
    protected Headers recordHeaders;

    /**
     * Parse from json string t.
     *
     * @param <T>  the type parameter
     * @param valueTypeRef the value type ref
     * @param response the response
     * @return the t
     */
    public static <T> T parseFromJsonString(TypeReference<T> valueTypeRef, String response) {
        try {
            return new ObjectMapper().readValue(response, valueTypeRef);
        }
        catch (JsonProcessingException e) {
            LOGGER.atError().setMessage("Something bad happened").setCause(e).log();
            return null;
        }
    }

    /**
     * To consumer record.
     *
     * @return the consumer record
     */
    public ConsumerRecord<String, String> toConsumerRecord() {
        return new ConsumerRecord<>(
                this.topic,
                this.partition,
                this.offset,
                this.timestamp,
                KafkaUtils.getTimestampType(this.timestampType),
                -1,
                -1,
                this.key,
                String.valueOf(this.payload),
                recordHeaders,
                Optional.ofNullable(this.leaderEpoch));
    }
}
