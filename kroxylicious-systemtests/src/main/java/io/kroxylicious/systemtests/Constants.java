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
     * The deployment name for kroxylicous
     */
    String KROXY_DEPLOYMENT_NAME = "kroxylicious-proxy";
    /**
     * The service name for kroxylicious. Used for the bootstrap url
     */
    String KROXY_SERVICE_NAME = "kroxylicious-service";
    /**
     * The constant KROXY_CONFIG_NAME.
     */
    String KROXY_CONFIG_NAME = "kroxylicious-config";
    /**
     * Strimzi cluster operator deployment name
     */
    String STRIMZI_DEPLOYMENT_NAME = "strimzi-cluster-operator";
    /**
     * The default namespace used for kubernetes deployment
     */
    String KROXY_DEFAULT_NAMESPACE = "kafka";

    /**
     * The cert-manager namespace for kubernetes deployment
     */
    String CERT_MANAGER_NAMESPACE = "cert-manager";

    /**
     * API versions of Strimzi CustomResources
     */
    String KAFKA_API_VERSION_V1BETA2 = "kafka.strimzi.io/v1beta2";

    /**
     * Kind of Strimzi CustomResources
     */
    String KAFKA_KIND = "Kafka";
    /**
     * Kind of kafka topics
     */
    String KAFKA_TOPIC_KIND = "KafkaTopic";
    /**
     * Kind of kafka users
     */
    String KAFKA_USER_KIND = "KafkaUser";
    /**
     * Kind of kafka node pools
     */
    String KAFKA_NODE_POOL_KIND = "KafkaNodePool";
    /**
     * Kind of pods
     */
    String POD_KIND = "Pod";

    /**
     * Kind of config maps
     */
    String CONFIG_MAP_KIND = "ConfigMap";

    /**
     * Kind of services
     */
    String SERVICE_KIND = "Service";

    /**
     * Load balancer type name.
     */
    String LOAD_BALANCER_TYPE = "LoadBalancer";

    /**
     * Listener names for Kafka cluster
     */
    String PLAIN_LISTENER_NAME = "plain";
    /**
     * Listener name for tls
     */
    String TLS_LISTENER_NAME = "tls";

    /**
     * Strimzi related labels and annotations
     */
    String STRIMZI_DOMAIN = "strimzi.io/";
    /**
     * Strimzi cluster label
     */
    String STRIMZI_CLUSTER_LABEL = STRIMZI_DOMAIN + "cluster";

    /**
     * Polls and timeouts constants
     */
    long POLL_INTERVAL_FOR_RESOURCE_READINESS_MILLIS = Duration.ofSeconds(5).toMillis();
    /**
     * Poll interval for resource deletion in milliseconds
     */
    long POLL_INTERVAL_FOR_RESOURCE_DELETION_MILLIS = Duration.ofSeconds(1).toMillis();

    /**
     * Global timeout in milliseconds
     */
    long GLOBAL_TIMEOUT_MILLIS = Duration.ofMinutes(5).toMillis();
    /**
     * Global Poll interval in milliseconds
     */
    long GLOBAL_POLL_INTERVAL_MILLIS = Duration.ofSeconds(1).toMillis();

    /**
     * Kubernetes related constants
     */
    String DEPLOYMENT = "Deployment";
    /**
     * Strimzi kafka image url in quay
     */
    String STRIMZI_KAFKA_IMAGE = "quay.io/strimzi/kafka:latest-kafka-" + KAFKA_VERSION_DEFAULT;

    /**
     * The cert manager url to install it on kubernetes
     */
    String CERT_MANAGER_URL = "https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml";
    /**
     * kafka consumer client label to identify the consumer test client
     */
    String KAFKA_CONSUMER_CLIENT_LABEL = "kafka-consumer-client";
    /**
     * kafka producer client label to identify the producer test client
     */
    String KAFKA_PRODUCER_CLIENT_LABEL = "kafka-producer-client";
}
