/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.test.kafkacluster.extension;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import io.kroxylicious.test.kafkacluster.KafkaCluster;

/**
 * {@code @BrokerCluster} can be used to annotate a field in a test class or a
 *  parameter in a lifecycle method or test method of type {@link KafkaCluster}
 *  that should be resolved into a temporary Kafka cluster.
 *
 * <p>A simple example looks like:</p>
 * <pre>{@code
 * @ExtendWith(KafkaClusterExtension.class)
 * public class MyTest {
 *     @Test
 *     public void testKafkaClusterParameter(
 *             @BrokerCluster(numBrokers = 3) KafkaCluster cluster) {
 *         //... your test code using `cluster`
 *     }
 * }
 * }</pre>
 *
 * <p>Alternatively annotate a {@code static} or member field:</p>
 * <pre>{@code
 * @ExtendWith(KafkaClusterExtension.class)
 * public class MyTest {
 *     @BrokerCluster(numBrokers = 3) KafkaCluster cluster;
 *     @Test
 *     public void testKafkaClusterParameter() {
 *         //... your test code using `cluster`
 *     }
 * }
 * }</pre>
 *
 * <p>By default a KRaft cluster with a single broker and colocated controller
 * will be used using containers.
 * However, the configuration of the cluster
 * can be influenced with a number of other annotations:</p>
 * <dl>
 *     <dt>{@link KRaftCluster}</dt><dd>Allow specifying the number of KRaft controllers</dd>
 *     <dt>{@link ZooKeeperCluster}</dt><dd>Allow specifying that a ZK-based cluster should be used</dd>
 *     <dt>{@link SaslPlainAuth}</dt><dd>Will configure the cluster for SASL-PLAIN authentication</dd>
 * </dl>
 *
 * <p>For example:</p>
 * <pre>{@code
 * @ExtendWith(KafkaClusterExtension.class)
 * public class MyTest {
 *     @Test
 *     public void testKafkaClusterParameter(
 *             @BrokerCluster(numBrokers = 3)
 *             @KRaftCluster(numControllers = 3)
 *             @SaslPlainAuth({
 *                 @UserPassword(user="alice", password="foo"),
 *                 @UserPassword(user="bob", password="bar")
 *             })
 *             KafkaCluster cluster) {
 *         //... your test code using `cluster`
 *     }
 * }
 * }</pre>
 *
 * @see KafkaClusterExtension
 */
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface BrokerCluster {
    /**
     * @return The number of brokers in the cluster
     */
    public int numBrokers() default 1;

}
