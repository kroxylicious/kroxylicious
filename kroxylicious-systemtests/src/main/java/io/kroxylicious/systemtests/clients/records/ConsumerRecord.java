/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.records;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ConsumerRecord {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerRecord.class);

    protected String topic;
    protected String key;
    protected String payload;
    protected int partition;
    protected long offset;
    protected Map<String, String> recordHeaders;

    public String getTopic() {
        return topic;
    }

    public String getKey() {
        return key;
    }

    public String getPayload() {
        return payload;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public Map<String, String> getRecordHeaders() {
        return recordHeaders;
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
            return new ObjectMapper().readValue(response, valueTypeRef);
        }
        catch (JsonProcessingException e) {
            LOGGER.atError().setMessage("Something bad happened").setCause(e).log();
            return null;
        }
    }
}
