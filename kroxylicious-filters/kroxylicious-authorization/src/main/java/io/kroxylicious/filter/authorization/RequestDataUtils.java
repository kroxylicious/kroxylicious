/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.Iterator;
import java.util.Objects;

import io.kroxylicious.kafka.common.message.ProduceRequestData;
import io.kroxylicious.kafka.common.record.internal.RecordBatch;
import io.kroxylicious.kafka.common.record.internal.Records;

/**
 * Utility methods for working with Kafka {@code *RequestData} classes.
 */
public class RequestDataUtils {

    /**
     * Prevent construction of utility class
     */
    private RequestDataUtils() {
        // prevent construction
    }

    /**
     * Mirrors the logic of
     * kafka-clients {@code org.apache.kafka.common.requests.RequestUtils#hasTransactionalRecords(ProduceRequest)}
     * We prefer to only depend on the *Data classes and their dependencies to try and control our exposure
     * to internal classes.
     * @param requestData request data
     * @return true if any RecordBatch in the request is transactional
     */
    public static boolean hasTransactionalRecords(ProduceRequestData requestData) {
        for (ProduceRequestData.TopicProduceData tp : Objects.requireNonNull(requestData).topicData()) {
            for (ProduceRequestData.PartitionProduceData p : tp.partitionData()) {
                if (p.records() instanceof Records records) {
                    Iterator<? extends RecordBatch> iter = records.batchIterator();
                    if (iter.hasNext() && iter.next().isTransactional()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
