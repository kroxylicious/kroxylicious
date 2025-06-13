/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;

import static io.micrometer.core.instrument.Metrics.globalRegistry;

public class RecordEncryptionMetrics {

    public static final String VIRTUAL_CLUSTER_LABEL = "virtual_cluster";
    public static final String TOPIC_NAME = "topic_name";

    // Base Metric Names
    protected static final String KROXYLICIOUS_ENCRYPTED_RECORDS_BASE_METER_NAME = "kroxylicious_encrypted_records_total";
    protected static final String KROXYLICIOUS_NOT_ENCRYPTED_RECORDS_BASE_METER_NAME = "kroxylicious_not_encrypted_records_total";

    // Meter Providers
    // (Callers use the providers to create the meters they need, augmenting with any tags).
    public static final ClusterNodeSpecificMetricProviderCreator<Counter> KROXYLICIOUS_ENCRYPTED_RECORDS_BASE_METER_NAME_TOTAL_METER_PROVIDER = (virtualClusterName,
                                                                                                                                                 nodeId) -> Counter
                                                                                                                                                         .builder(
                                                                                                                                                                 KROXYLICIOUS_ENCRYPTED_RECORDS_BASE_METER_NAME)
                                                                                                                                                         .description(
                                                                                                                                                                 "Incremented by the number of records encrypted.")
                                                                                                                                                         .tag(VIRTUAL_CLUSTER_LABEL,
                                                                                                                                                                 virtualClusterName)
                                                                                                                                                         .tag(TOPIC_NAME,
                                                                                                                                                                 nodeId)
                                                                                                                                                         .withRegistry(
                                                                                                                                                                 globalRegistry);

    public static final ClusterNodeSpecificMetricProviderCreator<Counter> KROXYLICIOUS_NOT_ENCRYPTED_RECORDS_TOTAL_METER_PROVIDER = (virtualClusterName,
                                                                                                                                     nodeId) -> Counter.builder(
                                                                                                                                             KROXYLICIOUS_NOT_ENCRYPTED_RECORDS_BASE_METER_NAME)
                                                                                                                                             .description(
                                                                                                                                                     "Incremented by the number of records not encrypted.")
                                                                                                                                             .tag(VIRTUAL_CLUSTER_LABEL,
                                                                                                                                                     virtualClusterName)
                                                                                                                                             .tag(TOPIC_NAME,
                                                                                                                                                     nodeId)
                                                                                                                                             .withRegistry(
                                                                                                                                                     globalRegistry);

    @FunctionalInterface
    public interface ClusterNodeSpecificMetricProviderCreator<T extends Meter> {

        Meter.MeterProvider<T> create(String virtualCluster, String nodeId);
    }
}