/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.records;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import io.kroxylicious.systemtests.utils.KafkaUtils;

/**
 * The type Kcat consumer record.
 */
public class KcatConsumerRecord extends BaseConsumerRecord {
    private long ts;
    private String tstype;
    private int broker;
    private List<String> headers;

    /**
     * Sets broker.
     *
     * @param broker the broker
     */
    public void setBroker(int broker) {
        this.broker = broker;
    }

    /**
     * Sets ts.
     *
     * @param timestamp the timestamp
     */
    public void setTs(long timestamp) {
        this.ts = timestamp;
    }

    /**
     * Sets tstype.
     *
     * @param timestampType the timestamp type
     */
    public void setTstype(String timestampType) {
        this.tstype = timestampType;
    }

    /**
     * Sets headers.
     *
     * @param headers the headers
     */
    public void setHeaders(List<String> headers) {
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
            assert this.headers.size() % 2 == 0;
            for (int i = 0; i < this.headers.size(); i += 2) {
                recordHeaders.add(this.headers.get(i), Optional.ofNullable(this.headers.get(i + 1)).orElse("").getBytes(StandardCharsets.UTF_8));
            }
        }
        return new ConsumerRecord<>(
                this.topic,
                this.partition,
                this.offset,
                this.ts,
                KafkaUtils.getTimestampType(this.tstype),
                -1,
                -1,
                (String) this.key,
                String.valueOf(this.payload),
                recordHeaders,
                Optional.ofNullable(this.leaderEpoch));
    }
}
