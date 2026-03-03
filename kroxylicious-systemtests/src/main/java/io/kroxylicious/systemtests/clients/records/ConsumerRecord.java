/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.records;

import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class ConsumerRecord {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRecord.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    protected String topic;
    protected String key;
    protected String payload;
    protected int partition;
    protected long offset;
    protected Map<String, String> recordHeaders;

    protected ConsumerRecord(String topic, String key, String payload, int partition, long offset) {
        this.topic = topic;
        this.key = key;
        this.payload = payload;
        this.partition = partition;
        this.offset = offset;
    }

    public String getTopic() {
        return topic;
    }

    public String getKey() {
        return key;
    }

    public String getPayload() {
        return payload;
    }

    public Map<String, String> getRecordHeaders() {
        return recordHeaders;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConsumerRecord that = (ConsumerRecord) o;
        return Objects.equals(topic, that.topic) &&
                Objects.equals(key, that.key) &&
                Objects.equals(payload, that.payload) &&
                partition == that.partition &&
                offset == that.offset &&
                Objects.deepEquals(recordHeaders, that.recordHeaders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, key, payload, partition, offset, recordHeaders);
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
        try {
            return OBJECT_MAPPER.readValue(response, valueTypeRef);
        }
        catch (JsonProcessingException e) {
            LOGGER.atError().setMessage("Something bad happened").setCause(e).log();
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString() {
        return "ConsumerRecord(topic: " + this.topic +
                ", key: " + this.key +
                ", payload: " + this.payload +
                ", partition: " + this.partition +
                ", offset: " + this.offset +
                ", headers: " + this.recordHeaders +
                ")";
    }
}
