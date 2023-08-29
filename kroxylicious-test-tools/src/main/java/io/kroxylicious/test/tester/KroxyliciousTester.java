/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.io.Closeable;
import java.util.Map;

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
     * for the only virtual cluster configured.
     * @return Admin client
     * @throws AmbiguousVirtualClusterException if this tester is for a Kroxylicious configured with multiple virtual clusters
     */
    Admin admin();

    /**
     * Creates an Admin Client configured with the kroxylicious bootstrap server
     * for a specific virtual cluster.
     * @param virtualCluster the virtual cluster we want the client to connect to
     * @return Admin client
     * @throws IllegalArgumentException if the named virtual cluster is not part of the kroxylicious server
     */
    Admin admin(String virtualCluster);

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
     * for a specific virtual cluster.
     * @param virtualCluster the virtual cluster we want the client to connect to
     * @return Producer
     * @throws IllegalArgumentException if the named virtual cluster is not part of the kroxylicious server
     */
    Producer<String, String> producer(String virtualCluster);

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

}
