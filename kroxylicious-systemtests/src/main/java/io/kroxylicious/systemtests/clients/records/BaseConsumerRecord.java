/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.records;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class BaseConsumerRecord {
    private static final Logger LOGGER = LoggerFactory.getLogger(BaseConsumerRecord.class);

    protected String topic;
    protected Object key;
    protected Object payload;
    protected int partition;
    protected long offset;
    protected Integer leaderEpoch;

    /**
     * Sets topic.
     *
     * @param topic the topic
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * Sets key.
     *
     * @param key the key
     */
    public void setKey(Object key) {
        this.key = key;
    }

    /**
     * Sets payload.
     *
     * @param payload the payload
     */
    public void setPayload(Object payload) {
        this.payload = payload;
    }

    /**
     * Sets partition.
     *
     * @param partition the partition
     */
    public void setPartition(int partition) {
        this.partition = partition;
    }

    /**
     * Sets offset.
     *
     * @param offset the offset
     */
    public void setOffset(long offset) {
        this.offset = offset;
    }

    /**
     * Sets leader epoch.
     *
     * @param leaderEpoch the leader epoch
     */
    public void setLeaderEpoch(Integer leaderEpoch) {
        this.leaderEpoch = leaderEpoch;
    }

    /**
     * Parse from json string t.
     *
     * @param <T>  the type parameter
     * @param valueTypeRef the value type ref
     * @param response the response
     * @return the t
     */
    public static <T> T parseFromJsonString(TypeReference<T> valueTypeRef, String response) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(response, valueTypeRef);
        }
        catch (JsonProcessingException e) {
            LOGGER.atError().setMessage("Something bad happened").setCause(e).log();
            return null;
        }
    }
}
