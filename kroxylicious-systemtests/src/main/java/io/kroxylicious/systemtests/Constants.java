/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.time.Duration;

import static io.kroxylicious.systemtests.Environment.KAFKA_VERSION_DEFAULT;

/**
 * The interface Constants.
 */
public interface Constants {

    /**
     * The constant KROXY_DEPLOYMENT_NAME.
     */
    String KROXY_DEPLOYMENT_NAME = "kroxylicious-proxy";
    /**
     * The constant KROXY_SERVICE_NAME.
     */
    String KROXY_SERVICE_NAME = "kroxylicious-service";
    /**
     * Strimzi related constants
     */
    String STRIMZI_DEPLOYMENT_NAME = "strimzi-cluster-operator";
    /**
     * The constant KROXY_DEFAULT_NAMESPACE.
     */
    String KROXY_DEFAULT_NAMESPACE = "kafka";

    /**
     * API versions of Strimzi CustomResources
     */
    String KAFKA_API_VERSION_V1BETA2 = "kafka.strimzi.io/v1beta2";

    /**
     * Kind of Strimzi CustomResources
     */
    String KAFKA_KIND = "Kafka";
    /**
     * The constant KAFKA_TOPIC_KIND.
     */
    String KAFKA_TOPIC_KIND = "KafkaTopic";
    /**
     * The constant KAFKA_USER_KIND.
     */
    String KAFKA_USER_KIND = "KafkaUser";
    /**
     * The constant KAFKA_NODE_POOL_KIND.
     */
    String KAFKA_NODE_POOL_KIND = "KafkaNodePool";
    /**
     * The constant POD_KIND.
     */
    String POD_KIND = "Pod";

    /**
     * Listener names for Kafka cluster
     */
    String PLAIN_LISTENER_NAME = "plain";
    /**
     * The constant TLS_LISTENER_NAME.
     */
    String TLS_LISTENER_NAME = "tls";

    /**
     * Strimzi related labels and annotations
     */
    String STRIMZI_DOMAIN = "strimzi.io/";
    /**
     * The constant STRIMZI_CLUSTER_LABEL.
     */
    String STRIMZI_CLUSTER_LABEL = STRIMZI_DOMAIN + "cluster";

    /**
     * Polls and timeouts constants
     */
    long POLL_INTERVAL_FOR_RESOURCE_READINESS_MILLIS = Duration.ofSeconds(5).toMillis();
    /**
     * The constant POLL_INTERVAL_FOR_RESOURCE_DELETION_MILLIS.
     */
    long POLL_INTERVAL_FOR_RESOURCE_DELETION_MILLIS = Duration.ofSeconds(1).toMillis();

    /**
     * The constant GLOBAL_TIMEOUT_MILLIS.
     */
    long GLOBAL_TIMEOUT_MILLIS = Duration.ofMinutes(5).toMillis();
    /**
     * The constant GLOBAL_POLL_INTERVAL_MILLIS.
     */
    long GLOBAL_POLL_INTERVAL_MILLIS = Duration.ofSeconds(1).toMillis();

    /**
     * Kubernetes related constants
     */
    String DEPLOYMENT = "Deployment";
    /**
     * The constant KROXY_KUBE_DIR_PORTPERBROKER.
     */
    String KROXY_KUBE_DIR_PORTPERBROKER = "/kubernetes-examples/portperbroker_plain";
    /**
     * The constant KROXY_KUBE_DIR_MULTITENANT.
     */
    String KROXY_KUBE_DIR_MULTITENANT = "/kubernetes-examples/multitenant";
    /**
     * The constant STRIMZI_KAFKA_IMAGE.
     */
    String STRIMZI_KAFKA_IMAGE = "quay.io/strimzi/kafka:latest-kafka-" + KAFKA_VERSION_DEFAULT;
    /**
     * The constant KROXY_BOOTSTRAP.
     */
    String KROXY_BOOTSTRAP = KROXY_SERVICE_NAME + ":9292";
}
