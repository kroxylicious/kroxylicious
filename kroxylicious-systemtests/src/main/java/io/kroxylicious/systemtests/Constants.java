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
public interface Constants {

    /**
     * The deployment name for kroxylicous
     */
    String KROXYLICIOUS = "kroxylicious";
    String KROXY_DEPLOYMENT_NAME = "kroxylicious-proxy";
    String KO_DEPLOYMENT_NAME = "kroxylicious-operator";
    String KO_NAMESPACE = "kroxylicious-operator";

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
    Duration KO_OPERATION_TIMEOUT_DEFAULT = Duration.ofMinutes(5);
    Duration KO_OPERATION_TIMEOUT_SHORT = Duration.ofSeconds(30);
    Duration KO_OPERATION_TIMEOUT_MEDIUM = Duration.ofMinutes(2);

    /**
     * Kubernetes related constants
     */
    String DEPLOYMENT = "Deployment";
    String DEPLOYMENT_TYPE = "deployment-type";
    String CUSTOM_RESOURCE_DEFINITION = "CustomResourceDefinition";
    String CLUSTER_ROLE = "ClusterRole";
    String CLUSTER_ROLE_BINDING = "ClusterRoleBinding";
    String CONFIG_MAP_KIND = "ConfigMap";
    String CUSTOM_RESOURCE_DEFINITION_SHORT = "Crd";
    String JOB = "Job";
    String NAMESPACE = "Namespace";
    String POD_KIND = "Pod";
    String ROLE = "Role";
    String SECRET_KIND = "Secret";
    String SERVICE_KIND = "Service";
    String SERVICE_ACCOUNT = "ServiceAccount";

    /**
     * Test clients image url
     */
    String TEST_CLIENTS_IMAGE = "quay.io/strimzi-test-clients/test-clients:latest-kafka-" + Environment.KAFKA_VERSION;
    String KCAT_CLIENT_IMAGE = "quay.io/kroxylicious/kcat:1.7.1";
    String KAF_CLIENT_IMAGE = "quay.io/kroxylicious/kaf:v0.2.7";

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
     * Image pull policies
     */
    String PULL_IMAGE_IF_NOT_PRESENT = "IfNotPresent";
    String PULL_IMAGE_ALWAYS = "Always";

    /**
     * Restart policies
     */
    String RESTART_POLICY_ONFAILURE = "OnFailure";
    String RESTART_POLICY_NEVER = "Never";

    /**
     * Scraper pod labels
     */
    String SCRAPER_LABEL_KEY = "user-test-app";
    String SCRAPER_LABEL_VALUE = "scraper";
    String SCRAPER_NAME = "Scraper";
    String DEPLOYMENT_TYPE_LABEL_KEY = "deployment-type";

    /**
     * Basic paths to examples
     */
    String PATH_TO_OPERATOR = TestUtils.USER_PATH + "/../kroxylicious-operator";
    String PATH_TO_OPERATOR_EXAMPLES = PATH_TO_OPERATOR + "/examples";
    String PATH_TO_OPERATOR_INSTALL_FILES = PATH_TO_OPERATOR + "/install";

    /**
     * Auxiliary variable for kroxylicious operator deployment
     */
    String WATCH_ALL_NAMESPACES = "*";

    /**
     * Auxiliary variables for storing data across our tests
     */
    String PREPARE_OPERATOR_ENV_KEY = "PREPARE_OPERATOR_ENV";
}
