/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.time.Duration;

import io.kroxylicious.systemtests.utils.TestUtils;

/**
 * The interface Constants.
 */
public final class Constants {

    private Constants() {
    }

    /**
     * The deployment name for kroxylicous
     */
    public static final String KROXYLICIOUS = "kroxylicious";
    public static final String KO_DEPLOYMENT_NAME = "kroxylicious-operator";
    public static final String KO_NAMESPACE = "kroxylicious-operator";
    public static final String KROXY_DEPLOYMENT_NAME = "kroxylicious-proxy";
    /**
     * The service name for kroxylicious. Used for the bootstrap url
     */
    public static final String KROXY_SERVICE_NAME = "kroxylicious-service";
    /**
     * The constant KROXY_CONFIG_NAME.
     */
    public static final String KROXY_CONFIG_NAME = "kroxylicious-config";
    /**
     * Strimzi cluster operator deployment name
     */
    public static final String STRIMZI_DEPLOYMENT_NAME = "strimzi-cluster-operator";
    /**
     * The default namespace used for kubernetes deployment
     */
    public static final String KAFKA_DEFAULT_NAMESPACE = "kafka";

    /**
     * The cert-manager namespace for kubernetes deployment
     */
    public static final String CERT_MANAGER_NAMESPACE = "cert-manager";

    /**
     * API versions of Strimzi CustomResources
     */
    public static final String KAFKA_API_VERSION_V1BETA2 = "kafka.strimzi.io/v1beta2";

    /**
     * Kind of Strimzi CustomResources
     */
    public static final String KAFKA_KIND = "Kafka";

    /**
     * Kind of kafka users
     */
    public static final String KAFKA_USER_KIND = "KafkaUser";
    /**
     * Kind of kafka node pools
     */
    public static final String KAFKA_NODE_POOL_KIND = "KafkaNodePool";

    /**
     * Load balancer type name.
     */
    public static final String LOAD_BALANCER_TYPE = "LoadBalancer";

    /**
     * Listener names for Kafka cluster
     */
    public static final String PLAIN_LISTENER_NAME = "plain";
    /**
     * Listener name for tls
     */
    public static final String TLS_LISTENER_NAME = "tls";

    /**
     * Strimzi related labels and annotations
     */
    public static final String STRIMZI_DOMAIN = "strimzi.io/";
    /**
     * Strimzi cluster label
     */
    public static final String STRIMZI_CLUSTER_LABEL = STRIMZI_DOMAIN + "cluster";

    /**
     * Polls and timeouts constants
     */
    public static final Duration POLL_INTERVAL_FOR_RESOURCE_READINESS = Duration.ofSeconds(2);
    /**
     * Poll interval for resource deletion
     */
    public static final Duration POLL_INTERVAL_FOR_RESOURCE_DELETION = Duration.ofSeconds(1);

    /**
     * Global timeout
     */
    public static final Duration GLOBAL_TIMEOUT = Duration.ofMinutes(5);
    /**
     * Global Poll interval
     */
    public static final Duration GLOBAL_POLL_INTERVAL = Duration.ofSeconds(1);
    public static final Duration RECONCILIATION_INTERVAL = Duration.ofSeconds(30);
    public static final Duration GLOBAL_POLL_INTERVAL_MEDIUM = Duration.ofSeconds(10);
    public static final Duration GLOBAL_STATUS_TIMEOUT = Duration.ofMinutes(3);
    public static final Duration GLOBAL_TIMEOUT_SHORT = Duration.ofMinutes(2);
    public static final Duration KO_OPERATION_TIMEOUT_DEFAULT = Duration.ofMinutes(5);
    public static final Duration KO_OPERATION_TIMEOUT_SHORT = Duration.ofSeconds(30);
    public static final Duration KO_OPERATION_TIMEOUT_MEDIUM = Duration.ofMinutes(2);

    /**
     * Kubernetes related constants
     */
    public static final String DEPLOYMENT = "Deployment";
    public static final String DEPLOYMENT_TYPE = "deployment-type";
    public static final String CUSTOM_RESOURCE_DEFINITION = "CustomResourceDefinition";
    public static final String CLUSTER_ROLE = "ClusterRole";
    public static final String CLUSTER_ROLE_BINDING = "ClusterRoleBinding";
    public static final String CONFIG_MAP_KIND = "ConfigMap";
    public static final String CUSTOM_RESOURCE_DEFINITION_SHORT = "Crd";
    public static final String JOB = "Job";
    public static final String NAMESPACE = "Namespace";
    public static final String POD_KIND = "Pod";
    public static final String ROLE = "Role";
    public static final String SECRET_KIND = "Secret";
    public static final String SERVICE_KIND = "Service";
    public static final String SERVICE_ACCOUNT = "ServiceAccount";

    /**
     * Test clients image url
     */
    public static final String TEST_CLIENTS_IMAGE = "quay.io/strimzi-test-clients/test-clients:latest-kafka-" + Environment.KAFKA_VERSION;
    public static final String KCAT_CLIENT_IMAGE = "quay.io/kroxylicious/kcat:1.7.1";
    public static final String KAF_CLIENT_IMAGE = "quay.io/kroxylicious/kaf:v0.2.7";

    /**
     * The cert manager url to install it on kubernetes
     */
    public static final String CERT_MANAGER_URL = "https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml";
    /**
     * kafka consumer client label to identify the consumer test client
     */
    public static final String KAFKA_CONSUMER_CLIENT_LABEL = "kafka-consumer-client";
    /**
     * kafka producer client label to identify the producer test client
     */
    public static final String KAFKA_PRODUCER_CLIENT_LABEL = "kafka-producer-client";
    /**
     * kafka admin client label to identify the admin test client
     */
    public static final String KAFKA_ADMIN_CLIENT_LABEL = "admin-client-cli";
    /**
     * Image pull policies
     */
    public static final String PULL_IMAGE_IF_NOT_PRESENT = "IfNotPresent";
    public static final String PULL_IMAGE_ALWAYS = "Always";

    /**
     * Restart policies
     */
    public static final String RESTART_POLICY_ONFAILURE = "OnFailure";
    public static final String RESTART_POLICY_NEVER = "Never";

    /**
     * Scraper pod labels
     */
    public static final String SCRAPER_LABEL_KEY = "user-test-app";
    public static final String SCRAPER_LABEL_VALUE = "scraper";
    public static final String SCRAPER_NAME = "Scraper";
    public static final String DEPLOYMENT_TYPE_LABEL_KEY = "deployment-type";

    /**
     * Basic paths to examples
     */
    public static final String PATH_TO_OPERATOR = TestUtils.USER_PATH + "/../kroxylicious-operator";
    public static final String PATH_TO_OPERATOR_EXAMPLES = PATH_TO_OPERATOR + "/examples";
    public static final String PATH_TO_OPERATOR_INSTALL_FILES = PATH_TO_OPERATOR + "/install";

    /**
     * Auxiliary variable for kroxylicious operator deployment
     */
    public static final String WATCH_ALL_NAMESPACES = "*";

    /**
     * Auxiliary variables for storing data across our tests
     */
    public static final String PREPARE_OPERATOR_ENV_KEY = "PREPARE_OPERATOR_ENV";

    public static final String DOCKER_REGISTRY_AWS_MIRROR = "public.ecr.aws/docker";
    public static final String DOCKER_REGISTRY_GCR_MIRROR = "mirror.gcr.io";
}
