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
    private static final String ENCRYPTED_RECORDS = "kroxylicious_filter_record_encryption_encrypted_records";
    private static final String PLAIN_RECORDS = "kroxylicious_filter_record_encryption_plain_records";

    public static Meter.MeterProvider<Counter> encryptedRecordsCounter(String clusterName) {
        return buildCounterMeterProvider(ENCRYPTED_RECORDS, "Incremented by the number of records encrypted.",
                clusterName);
    }

    public static Meter.MeterProvider<Counter> plainRecordsCounter(String clusterName) {
        return buildCounterMeterProvider(PLAIN_RECORDS, "Incremented by the number of records not encrypted.",
                clusterName);
    }

    @NonNull
    private static Meter.MeterProvider<Counter> buildCounterMeterProvider(String meterName,
                                                                          String description,
                                                                          String clusterName) {
        return Counter
                .builder(meterName)
                .description(description)
                .tag(VIRTUAL_CLUSTER_LABEL, clusterName)
                .withRegistry(globalRegistry);
    }
}
