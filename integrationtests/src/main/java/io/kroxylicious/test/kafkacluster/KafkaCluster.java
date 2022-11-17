/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.kafkacluster;

import java.util.Map;

public interface KafkaCluster extends AutoCloseable {
    /**
     * starts the cluster.
     */
    void start();

    /**
     * stops the cluster.
     */
    @Override
    void close() throws Exception;

    /**
     * Gets the bootstrap servers for this cluster
     * @return bootstrap servers
     */
    String getBootstrapServers();

    /**
     * @return The cluster id for KRaft-based clusters, otherwise null;
     */
    String getClusterId();

    /**
     * Gets the kafka configuration for making connections to this cluster as required by the
     * {@link org.apache.kafka.clients.admin.AdminClient}, {@link org.apache.kafka.clients.producer.Producer} etc.
     * Details such the bootstrap and SASL configuration are provided automatically.
     * The returned map is guaranteed to be mutable and is unique to the caller.
     *
     * @return mutable configuration map
     */
    Map<String, Object> getKafkaClientConfiguration();

    /**
     * Gets the kafka configuration for making connections to this cluster as required by the
     * {@link org.apache.kafka.clients.admin.AdminClient}, {@link org.apache.kafka.clients.producer.Producer} etc.
     * Details such the bootstrap and SASL configuration are provided automatically.
     * The returned map is guaranteed to be mutable and is unique to the caller.
     *
     * @param user The user
     * @param password The password
     * @return mutable configuration map
     */
    Map<String, Object> getKafkaClientConfiguration(String user, String password);
}
