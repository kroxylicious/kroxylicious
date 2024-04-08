/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.time.Duration;

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
    String KAFKA_DEFAULT_NAMESPACE = "kafka";

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
     * Kind of jobs
     */
    String JOB = "Job";

    /**
     * Kind of services
     */
    String SERVICE_KIND = "Service";

    /**
     * Kind of secret
     */
    String SECRET_KIND = "Secret";

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
    Duration POLL_INTERVAL_FOR_RESOURCE_READINESS = Duration.ofSeconds(2);
    /**
     * Poll interval for resource deletion
     */
    Duration POLL_INTERVAL_FOR_RESOURCE_DELETION = Duration.ofSeconds(1);

    /**
     * Global timeout
     */
    Duration GLOBAL_TIMEOUT = Duration.ofMinutes(5);
    /**
     * Global Poll interval
     */
    Duration GLOBAL_POLL_INTERVAL = Duration.ofSeconds(1);
    Duration RECONCILIATION_INTERVAL = Duration.ofSeconds(30);
    Duration GLOBAL_POLL_INTERVAL_MEDIUM = Duration.ofSeconds(10);
    Duration GLOBAL_STATUS_TIMEOUT = Duration.ofMinutes(3);
    Duration GLOBAL_TIMEOUT_SHORT = Duration.ofMinutes(2);

    /**
     * Kubernetes related constants
     */
    String DEPLOYMENT = "Deployment";

    /**
     * Strimzi kafka image url in quay
     */
    String STRIMZI_KAFKA_IMAGE = "quay.io/strimzi/kafka:latest-kafka-" + Environment.KAFKA_VERSION;
    /**
     * Test clients image url
     */
    String TEST_CLIENTS_IMAGE = "quay.io/strimzi-test-clients/test-clients:latest-kafka-" + Environment.KAFKA_VERSION;

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
    /**
     * kafka admin client label to identify the admin test client
     */
    String KAFKA_ADMIN_CLIENT_LABEL = "admin-client-cli";
    /**
     * Image pull if not present.
     */
    String PULL_IMAGE_IF_NOT_PRESENT = "IfNotPresent";

    /**
     * Feature gate related constants
     */
    String USE_KRAFT_MODE = "+UseKRaft";
    String DONT_USE_KRAFT_MODE = "-UseKRaft";
    String USE_KAFKA_NODE_POOLS = "+KafkaNodePools";
    String DONT_USE_KAFKA_NODE_POOLS = "-KafkaNodePools";

    /**
     * Scraper pod labels
     */
    String SCRAPER_LABEL_KEY = "user-test-app";
    String SCRAPER_LABEL_VALUE = "scraper";
    String SCRAPER_NAME = "Scraper";
    String DEPLOYMENT_TYPE_LABEL_KEY = "deployment-type";
}
