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
 */
@Target({ ElementType.PARAMETER, ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
public @interface BrokerCluster {
    /**
     * @return The number of brokers in the cluster
     */
    public int numBrokers() default 1;

}
