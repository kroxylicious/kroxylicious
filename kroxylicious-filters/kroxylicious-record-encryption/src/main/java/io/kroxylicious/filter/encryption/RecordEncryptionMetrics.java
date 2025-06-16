/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.micrometer.core.instrument.Metrics.globalRegistry;

public class RecordEncryptionMetrics {

    public static final String VIRTUAL_CLUSTER_LABEL = "virtual_cluster";
    public static final String TOPIC_NAME = "topic_name";

    // Base Metric Names
    private static final String RECORD_ENCRYPTION_ENCRYPTED_RECORDS = "kroxylicious_record_encryption_encrypted_records";
    private static final String RECORD_ENCRYPTION_PLAIN_RECORDS = "kroxylicious_record_encryption_plain_records";

    public static Meter.MeterProvider<Counter> recordEncryptionEncryptedRecordsCounter(String clusterName, String topicName) {
        return buildCounterMeterProvider(RECORD_ENCRYPTION_ENCRYPTED_RECORDS, "Incremented by the number of records encrypted.",
                clusterName, topicName);
    }

    public static Meter.MeterProvider<Counter> recordEncryptionPlainRecordsCounter(String clusterName, String topicName) {
        return buildCounterMeterProvider(RECORD_ENCRYPTION_PLAIN_RECORDS, "Incremented by the number of records not encrypted.",
                clusterName, topicName);
    }

    @NonNull
    private static Meter.MeterProvider<Counter> buildCounterMeterProvider(String meterName,
                                                                          String description,
                                                                          String clusterName,
                                                                          String topicName) {
        return Counter
                .builder(meterName)
                .description(description)
                .tag(VIRTUAL_CLUSTER_LABEL, clusterName)
                .tag(TOPIC_NAME, topicName)
                .withRegistry(globalRegistry);
    }
}
