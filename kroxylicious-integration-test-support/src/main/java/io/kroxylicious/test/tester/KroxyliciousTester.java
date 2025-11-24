/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serde;

import io.kroxylicious.test.client.KafkaClient;

/**
 * A convenient tester for a Kroxylicious instance. Implementations of this will
 * typically create a Kroxylicious server and clean it up on {@link #close()}. It provides
 * convenient methods for obtaining clients configured to talk to Kroxylicious.
 * KroxyliciousTester is virtual cluster aware, so you can ask it for a client
 * for a specific virtual cluster offered by the kroxylicious instance.
 */
public interface KroxyliciousTester extends Closeable {

    /**
     * Creates an Admin Client configured with the kroxylicious bootstrap server
     * for the only virtual cluster configured.
     * @param additionalConfig additional configuration for the Admin client
     * @return Admin client
     * @throws AmbiguousVirtualClusterException if this tester is for a Kroxylicious configured with multiple virtual clusters
     */
    Admin admin(Map<String, Object> additionalConfig);

    /**
     * Creates an Admin Client configured with the kroxylicious bootstrap server
     * for a specific virtual cluster.
     * @param virtualCluster the virtual cluster we want the client to connect to
     * @param additionalConfig additional configuration for the Admin client
     * @return Admin client
     * @throws IllegalArgumentException if the named virtual cluster is not part of the kroxylicious server
     */
    Admin admin(String virtualCluster, Map<String, Object> additionalConfig);

    /**
     * Creates an Admin Client configured with the kroxylicious bootstrap server
     * for a specific virtual cluster.
     * @param virtualCluster the virtual cluster we want the client to connect to
     * @param listenerName the listener we want the client to connect to
     * @param additionalConfig additional configuration for the Admin client
     * @return Admin client
     * @throws IllegalArgumentException if the named virtual cluster is not part of the kroxylicious server
     */
    Admin admin(String virtualCluster, String listenerName, Map<String, Object> additionalConfig);

    /**
     * Creates an Admin Client configured with the kroxylicious bootstrap server
     * for the only virtual cluster configured.
     * @return Admin client
     * @throws AmbiguousVirtualClusterException if this tester is for a Kroxylicious configured with multiple virtual clusters
     */
    Admin admin();

    /**
     * Creates an Admin Client configured with the kroxylicious bootstrap server
     * for a specific virtual cluster with a single listener.
     * @param virtualCluster the virtual cluster we want the client to connect to
     * @return Admin client
     * @throws IllegalArgumentException if the named virtual cluster is not part of the kroxylicious server
     */
    Admin admin(String virtualCluster);

    /**
     * Creates an Admin Client configured with the kroxylicious bootstrap server
     * for a specific listener on a specific virtual cluster
     * @param virtualCluster the virtual cluster we want the client to connect to
     * @param listener the listener we want the client to connect to
     * @return Admin client
     * @throws IllegalArgumentException if the named virtual cluster is not part of the kroxylicious server, or if the virtual cluster does not have a listener with this name
     */
    Admin admin(String virtualCluster, String listener);

    /**
     * Creates a Producer configured with the kroxylicious bootstrap server
     * for the only virtual cluster configured.
     * @param additionalConfig additional producer configuration
     * @return Producer
     * @throws AmbiguousVirtualClusterException if this tester is for a Kroxylicious configured with multiple virtual clusters
     */
    Producer<String, String> producer(Map<String, Object> additionalConfig);

    /**
     * Creates a Producer configured with the kroxylicious bootstrap server
     * for a specific virtual cluster.
     * @param virtualCluster the virtual cluster we want the client to connect to
     * @param additionalConfig additional producer configuration
     * @return Producer
     * @throws IllegalArgumentException if the named virtual cluster is not part of the kroxylicious server
     */
    Producer<String, String> producer(String virtualCluster, Map<String, Object> additionalConfig);

    /**
     * Creates a Producer configured with the kroxylicious bootstrap server
     * for the only virtual cluster configured.
     * @return Producer
     * @throws AmbiguousVirtualClusterException if this tester is for a Kroxylicious configured with multiple virtual clusters
     */
    Producer<String, String> producer();

    /**
     * Creates a Producer configured with the kroxylicious bootstrap server
     * for a specific virtual cluster for the default listener.
     * @param virtualCluster the virtual cluster we want the client to connect to
     * @return Producer
     * @throws IllegalArgumentException if the named virtual cluster is not part of the kroxylicious server
     */
    Producer<String, String> producer(String virtualCluster);

    /**
     * Creates a Producer configured with the kroxylicious bootstrap server
     * for a specific virtual cluster for the default listener.
     * @param virtualCluster the virtual cluster we want the client to connect to
     * @param listener the listener we want the client to connect to
     * @return Producer
     * @throws IllegalArgumentException if the named virtual cluster is not part of the kroxylicious server or if the virtual cluster doesn't have the named listener
     */
    Producer<String, String> producer(String virtualCluster, String listener);

    /**
     * Creates a Producer configured with the kroxylicious bootstrap server
     * for the only virtual cluster configured.
     * @param <U> key type
     * @param <V> value type
     * @param keySerde key serde
     * @param valueSerde value serde
     * @param additionalConfig additional produce config
     * @return Producer
     * @throws AmbiguousVirtualClusterException if this tester is for a Kroxylicious configured with multiple virtual clusters
     */
    <U, V> Producer<U, V> producer(Serde<U> keySerde, Serde<V> valueSerde, Map<String, Object> additionalConfig);

    /**
     * Creates a Producer configured with the kroxylicious bootstrap server
     * for a specific virtual cluster.
     * @param <U> key type
     * @param <V> value type
     * @param keySerde key serde
     * @param valueSerde value serde
     * @param additionalConfig additional produce config
     * @param virtualCluster the virtual cluster we want the client to connect to
     * @return Producer
     * @throws IllegalArgumentException if the named virtual cluster is not part of the kroxylicious server
     */
    <U, V> Producer<U, V> producer(String virtualCluster, Serde<U> keySerde, Serde<V> valueSerde, Map<String, Object> additionalConfig);

    /**
     * Creates a Consumer configured with the kroxylicious bootstrap server
     * for the only virtual cluster configured.
     * @param additionalConfig additional consumer config
     * @return Consumer
     * @throws AmbiguousVirtualClusterException if this tester is for a Kroxylicious configured with multiple virtual clusters
     */
    Consumer<String, String> consumer(Map<String, Object> additionalConfig);

    /**
     * Creates a Consumer configured with the kroxylicious bootstrap server
     * for a specific virtual cluster.
     * @param additionalConfig additional consumer config
     * @param virtualCluster the virtual cluster we want the client to connect to
     * @return Consumer
     * @throws IllegalArgumentException if the named virtual cluster is not part of the kroxylicious server
     */
    Consumer<String, String> consumer(String virtualCluster, Map<String, Object> additionalConfig);

    /**
     * Creates a Consumer configured with the kroxylicious bootstrap server
     * for the only virtual cluster configured. Also sets a random group id
     * and sets auto offset reset to "earliest".
     * @return Consumer
     * @throws AmbiguousVirtualClusterException if this tester is for a Kroxylicious configured with multiple virtual clusters
     */
    Consumer<String, String> consumer();

    /**
     * Creates a Consumer configured with the kroxylicious bootstrap server
     * for a specific virtual cluster. Also sets a random group id
     * and sets auto offset reset to "earliest".
     * @param virtualCluster the virtual cluster we want the client to connect to
     * @return Consumer
     * @throws IllegalArgumentException if the named virtual cluster is not part of the kroxylicious server
     */
    Consumer<String, String> consumer(String virtualCluster);

    /**
     * Creates a Consumer configured with the kroxylicious bootstrap server
     * for a specific virtual cluster and listener. Also sets a random group id
     * and sets auto offset reset to "earliest".
     * @param virtualCluster the virtual cluster we want the client to connect to
     * @param listener the listener to connect to
     * @return Consumer
     * @throws IllegalArgumentException if the named virtual cluster is not part of the kroxylicious server or if the virtual cluster doesn't have the named listener
     */
    Consumer<String, String> consumer(String virtualCluster, String listener);

    /**
     * Creates a Consumer configured with the kroxylicious bootstrap server
     * for the only virtual cluster configured.
     * @param <U> key type
     * @param <V> value type
     * @param keySerde key serde
     * @param valueSerde value serde
     * @param additionalConfig additional consumer config
     * @return Admin client
     * @throws AmbiguousVirtualClusterException if this tester is for a Kroxylicious configured with multiple virtual clusters
     */
    <U, V> Consumer<U, V> consumer(Serde<U> keySerde, Serde<V> valueSerde, Map<String, Object> additionalConfig);

    /**
     * Creates a Consumer configured with the kroxylicious bootstrap server
     * for a specific virtual cluster. Also sets a random group id
     * and sets auto offset reset to "earliest".
     * @param <U> key type
     * @param <V> value type
     * @param keySerde key serde
     * @param valueSerde value serde
     * @param additionalConfig additional consumer config
     * @param virtualCluster the virtual cluster we want the client to connect to
     * @return Consumer
     * @throws IllegalArgumentException if the named virtual cluster is not part of the kroxylicious server
     */
    <U, V> Consumer<U, V> consumer(String virtualCluster, Serde<U> keySerde, Serde<V> valueSerde, Map<String, Object> additionalConfig);

    /**
     * Creates a Mock Request client configured with the kroxylicious bootstrap server
     * for the only virtual cluster configured. This client can be used to send multiple
     * ApiMessage to kroxilicious and receive many ApiMessage responses.
     * @return KafkaClient
     * @throws AmbiguousVirtualClusterException if this tester is for a Kroxylicious configured with multiple virtual clusters
     */
    KafkaClient simpleTestClient();

    KafkaClient simpleTestClient(String address, boolean useTls);

    /**
     * Creates a Mock Request client configured with the kroxylicious bootstrap server
     * for a specific virtual cluster. This client can be used to send multiple
     * ApiMessage to kroxilicious and receive many ApiMessage responses.
     * @param virtualCluster the virtual cluster we want the client to connect to
     * @return KafkaClient
     * @throws IllegalArgumentException if the named virtual cluster is not part of the kroxylicious server
     */
    KafkaClient simpleTestClient(String virtualCluster);

    /**
     * Restarts the Kroxylicious server under test without closing any other resources.
     */
    void restartProxy();

    /**
     * Close the Kroxylicious server under test and any other resources that need cleaning.
     */
    void close();

    /**
     * Creates a single topic on the default Kafka cluster with a fixed partition count (1) and replication factor (1).
     * <p>
     * The number of partitions can be increased via {@link  Admin#createPartitions(Map) Admin.createParitions}.
     * The number of replicas can be increased via {@link Admin#alterPartitionReassignments(Map) Admin.alterParitionsReassignments} by altering the replica assignments.
     * See the <a href="https://kafka.apache.org/documentation/#basic_ops_increase_replication_factor"> Kafka docs</a> for details
     * <p>
     * @param clusterName the name of the virtual cluster on which to create the topic
     * @return the name of the created topic
     */
    default String createTopic(String clusterName) {
        return createTopics(clusterName, 1).stream().findFirst().orElseThrow(() -> new IllegalStateException("Failed to create topic"));
    }

    /**
     * Creates N topics with a fixed partition count (1) and replication factor (1).
     * <p>
     * The number of partitions can be increased via {@link  Admin#createPartitions(Map) Admin.createParitions}.
     * The number of replicas can be increased via {@link Admin#alterPartitionReassignments(Map) Admin.alterParitionsReassignments} by altering the replica assignments.
     * See the <a href="https://kafka.apache.org/documentation/#basic_ops_increase_replication_factor"> Kafka docs</a> for details
     * <p>
     * @param clusterName the name of the virtual cluster on which to create the topic
     * @param numberOfTopics the number of topics to create on the cluster
     * @return the Set of topic names which have been created.
     */
    Set<String> createTopics(String clusterName, int numberOfTopics);

    /**
     * Asks the tester to delete all topics <strong>created by</strong> the tester on a specific virtual cluster.
     * @param clusterName the name of the virtual cluster from which to delete the topics.
     */
    void deleteTopics(String clusterName);

    Map<String, Object> clientConfiguration();

    /**
     * @return the bootstrap address of the only virtual cluster
     * @throws AmbiguousVirtualClusterException if this tester is for a Kroxylicious configured with multiple virtual clusters
     */
    String getBootstrapAddress();

    /**
     * @return the bootstrap address of the named virtual cluster
     */
    String getBootstrapAddress(String clusterName, String listener);

    /**
     * @return the Admin Http Client
     * @throws IllegalStateException admin interface not available
     */
    ManagementClient getManagementClient();
}