/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious;

import java.time.Duration;

public interface Constants {
    /**
     * Strimzi related constants
     */
    String STRIMZI_DEPLOYMENT_NAME = "strimzi-cluster-operator";
    String STRIMZI_DEFAULT_NAMESPACE = "strimzi";

    /**
     * API versions of Strimzi CustomResources
     */
    String KAFKA_API_VERSION_V1BETA2 = "kafka.strimzi.io/v1beta2";

    /**
     * Kind of Strimzi CustomResources
     */
    String KAFKA_KIND = "Kafka";
    String KAFKA_TOPIC_KIND = "KafkaTopic";
    String KAFKA_USER_KIND = "KafkaUser";
    String KAFKA_NODE_POOL_KIND = "KafkaNodePool";
    String POD_KIND = "Pod";

    /**
     * Listener names for Kafka cluster
     */
    String PLAIN_LISTENER_NAME = "plain";
    String TLS_LISTENER_NAME = "tls";

    /**
     * Strimzi related labels and annotations
     */
    String STRIMZI_DOMAIN = "strimzi.io/";
    String STRIMZI_CLUSTER_LABEL = STRIMZI_DOMAIN + "cluster";

    /**
     * Polls and timeouts constants
     */
    long POLL_INTERVAL_FOR_RESOURCE_READINESS_MILLIS = Duration.ofSeconds(5).toMillis();
    long POLL_INTERVAL_FOR_RESOURCE_DELETION_MILLIS = Duration.ofSeconds(1).toMillis();

    long GLOBAL_TIMEOUT_MILLIS = Duration.ofMinutes(5).toMillis();
    long GLOBAL_POLL_INTERVAL_MILLIS = Duration.ofSeconds(1).toMillis();

    /**
     * Kubernetes related constants
     */
    String DEPLOYMENT = "Deployment";
}
