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
public final class Constants {

    private Constants() {
    }

    /**
     * The deployment name for kroxylicous
     */
    public static final String KROXYLICIOUS_OPERATOR_DEPLOYMENT_NAME = "kroxylicious-operator";
    public static final String KROXYLICIOUS_OPERATOR_NAMESPACE = "kroxylicious-operator";
    public static final String KROXYLICIOUS_DEPLOYMENT_NAME = "kroxylicious-proxy";
    public static final String KROXYLICIOUS_PROXY_SIMPLE_NAME = "simple";
    public static final String KROXYLICIOUS_INGRESS_CLUSTER_IP = "cluster-ip";
    public static final String KROXYLICIOUS_ENCRYPTION_FILTER_NAME = "encryption";
    public static final String KROXYLICIOUS_TLS_CLIENT_CA_CERT = "my-cluster-clients-ca-cert";
    public static final String KROXYLICIOUS_TLS_CA_NAME = "ca.pem";
    public static final String KROXYLICIOUS_OPERATOR_OLM_DEPLOYMENT_NAME = "amq-streams-proxy";
    public static final String KROXYLICIOUS_OPERATOR_SUBSCRIPTION_NAME = KROXYLICIOUS_OPERATOR_OLM_DEPLOYMENT_NAME + "-v" + Environment.KROXYLICIOUS_OPERATOR_VERSION + "-sub";
    public static final String KROXYLICIOUS_OPERATOR_OLM_LABEL = KROXYLICIOUS_OPERATOR_OLM_DEPLOYMENT_NAME + "-operator-v" + Environment.KROXYLICIOUS_OPERATOR_VERSION;

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

    /**
     * Kubernetes related constants
     */
    public static final String DEPLOYMENT = "Deployment";
    public static final String DEPLOYMENT_TYPE = "deployment-type";
    public static final String CLUSTER_ROLE = "ClusterRole";
    public static final String CONFIG_MAP = "ConfigMap";
    public static final String JOB = "Job";
    public static final String NAMESPACE = "Namespace";
    public static final String SECRET = "Secret";
    public static final String SERVICE = "Service";
    public static final String SERVICE_ACCOUNT = "ServiceAccount";
    public static final String STRIMZI_KAFKA_KIND = "Kafka";
    public static final String STRIMZI_KAFKA_NODE_POOL_KIND = "KafkaNodePool";
    public static final String KROXYLICIOUS_KAFKA_PROTOCOL_FILTER_KIND = "KafkaProtocolFilter";
    public static final String KROXYLICIOUS_KAFKA_PROXY_KIND = "KafkaProxy";
    public static final String KROXYLICIOUS_KAFKA_PROXY_INGRESS_KIND = "KafkaProxyIngress";
    public static final String KROXYLICIOUS_KAFKA_SERVICE_KIND = "KafkaService";
    public static final String KROXYLICIOUS_VIRTUAL_KAFKA_CLUSTER_KIND = "VirtualKafkaCluster";

    /**
     * Test clients image url
     */
    public static final String KCAT_CLIENT_IMAGE = "quay.io/kroxylicious/kcat:1.7.1";
    public static final String KAF_CLIENT_IMAGE = "quay.io/kroxylicious/kaf:v0.2.7";

    /**
     * The cert manager url to install it on kubernetes
     */
    public static final String CERT_MANAGER_URL = "https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml";
    /**
     * the kubernetes labels used to identify the test kafka clients pods
     */
    public static final String KAFKA_CONSUMER_CLIENT_LABEL = "kafka-consumer-client";
    public static final String KAFKA_PRODUCER_CLIENT_LABEL = "kafka-producer-client";
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
    public static final String CURL_IMAGE = Constants.DOCKER_REGISTRY_GCR_MIRROR + "/curlimages/curl:8.13.0";

    /**
     * Basic paths to examples
     */
    public static final String PATH_TO_CRDS = System.getProperty("user.dir") + "/../kroxylicious-kubernetes-api/src/main/resources/META-INF/fabric8/";
    public static final String PATH_TO_OPERATOR_INSTALL_FILES = System.getProperty("user.dir") + "/../kroxylicious-operator" + "/install";

    /**
     * Auxiliary variables for storing data across our tests
     */
    public static final String DOCKER_REGISTRY_GCR_MIRROR = "mirror.gcr.io";
}
