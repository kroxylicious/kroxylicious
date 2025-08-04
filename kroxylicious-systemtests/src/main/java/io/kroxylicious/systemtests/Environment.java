/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Properties;

import io.skodjob.testframe.enums.InstallType;
import io.skodjob.testframe.environment.TestEnvironmentVariables;

/**
 * The type Environment.
 */
public class Environment {

    private Environment() {
    }

    private static final TestEnvironmentVariables ENVIRONMENT_VARIABLES = new TestEnvironmentVariables();

    // ---------------------------------------
    // Env variables initialization
    // ---------------------------------------
    private static final String INSTALL_TYPE_ENV = "CLUSTER_OPERATOR_INSTALL_TYPE";
    public static final InstallType INSTALL_TYPE = ENVIRONMENT_VARIABLES.getOrDefault(INSTALL_TYPE_ENV, InstallType::fromString, InstallType.Yaml);

    /**
     * Env. variables names
     */
    private static final String KAFKA_VERSION_ENV = "KAFKA_VERSION";
    private static final String KROXYLICIOUS_OPERATOR_VERSION_ENV = "KROXYLICIOUS_OPERATOR_VERSION";
    public static final String SKIP_TEARDOWN_ENV = "SKIP_TEARDOWN";
    private static final String CONTAINER_CONFIG_PATH_ENV = "CONTAINER_CONFIG_PATH";
    private static final String SKIP_STRIMZI_INSTALL_ENV = "SKIP_STRIMZI_INSTALL";
    private static final String KAFKA_CLIENT_ENV = "KAFKA_CLIENT";
    private static final String STRIMZI_VERSION_ENV = "STRIMZI_VERSION";
    private static final String STRIMZI_NAMESPACE_ENV = "STRIMZI_NAMESPACE";
    private static final String CLUSTER_DUMP_DIR_ENV = "CLUSTER_DUMP_DIR";
    private static final String AWS_ACCESS_KEY_ID_ENV = "AWS_ACCESS_KEY_ID";
    private static final String AWS_SECRET_ACCESS_KEY_ENV = "AWS_SECRET_ACCESS_KEY";
    private static final String AWS_USE_CLOUD_ENV = "AWS_USE_CLOUD";
    private static final String AWS_KROXYLICIOUS_ACCESS_KEY_ID_ENV = "AWS_KROXYLICIOUS_ACCESS_KEY_ID";
    private static final String AWS_KROXYLICIOUS_SECRET_ACCESS_KEY_ENV = "AWS_KROXYLICIOUS_SECRET_ACCESS_KEY";
    private static final String AWS_REGION_ENV = "AWS_REGION";
    private static final String KROXYLICIOUS_OPERATOR_BUNDLE_IMAGE_ENV = "KROXYLICIOUS_OPERATOR_BUNDLE_IMAGE";
    private static final String TEST_CLIENTS_IMAGE_ENV = "TEST_CLIENTS_IMAGE";
    private static final String OLM_OPERATOR_CHANNEL_ENV = "OLM_OPERATOR_CHANNEL";
    private static final String CATALOG_SOURCE_NAME_ENV = "CATALOG_SOURCE_NAME";
    private static final String CATALOG_NAMESPACE_ENV = "CATALOG_NAMESPACE";
    private static final String KROXYLICIOUS_OLM_DEPLOYMENT_NAME_ENV = "KROXYLICIOUS_OLM_DEPLOYMENT_NAME";
    private static final String SYNC_RESOURCES_DELETION_ENV = "SYNC_RESOURCES_DELETION";
    private static final String TEST_CLIENTS_PULL_SECRET_ENV = "TEST_CLIENTS_PULL_SECRET";
    private static final String ARCHITECTURE_ENV = "ARCHITECTURE";
    private static final String KROXYLICIOUS_OPERATOR_INSTALL_DIR_ENV = "KROXYLICIOUS_OPERATOR_INSTALL_DIR";
    private static final String CURL_IMAGE_ENV = "CURL_IMAGE";

    /**
     * The kafka version default value
     */
    private static final String KAFKA_VERSION_DEFAULT;

    static {
        KAFKA_VERSION_DEFAULT = determineKafkaVersion();
    }

    /**
     * The kroxy version default value
     */
    private static final String KROXYLICIOUS_VERSION_DEFAULT;

    static {
        KROXYLICIOUS_VERSION_DEFAULT = determineKroxyliciousVersion();
    }

    /**
     * The kafka version default value
     */
    private static final String STRIMZI_VERSION_DEFAULT;

    static {
        STRIMZI_VERSION_DEFAULT = determineStrimziVersion();
    }

    /**
     * The default value for skipping the teardown locally.
     */
    private static final boolean SKIP_TEARDOWN_DEFAULT = false;
    private static final String CONTAINER_CONFIG_PATH_DEFAULT = System.getProperty("user.home") + "/.docker/config.json";
    private static final boolean SKIP_STRIMZI_INSTALL_DEFAULT = false;
    private static final String KAFKA_CLIENT_DEFAULT = "strimzi_test_client";
    private static final String CLUSTER_DUMP_DIR_DEFAULT = System.getProperty("user.dir") + "/target/logs/";
    public static final String AWS_ACCESS_KEY_ID_DEFAULT = "test";
    private static final String AWS_SECRET_ACCESS_KEY_DEFAULT = "test";
    private static final String AWS_USE_CLOUD_DEFAULT = "false";
    public static final String AWS_KROXYLICIOUS_ACCESS_KEY_ID_DEFAULT = AWS_ACCESS_KEY_ID_DEFAULT;
    private static final String AWS_KROXYLICIOUS_SECRET_ACCESS_KEY_DEFAULT = AWS_SECRET_ACCESS_KEY_DEFAULT;
    public static final String AWS_REGION_DEFAULT = "us-east-2";
    private static final String TEST_CLIENTS_IMAGE_DEFAULT = "quay.io/strimzi-test-clients/test-clients:latest-kafka-" + KAFKA_VERSION_DEFAULT;
    private static final String OLM_OPERATOR_CHANNEL_DEFAULT = "alpha";
    private static final String CATALOG_SOURCE_NAME_DEFAULT = "kroxylicious-source";
    private static final String KROXYLICIOUS_OLM_DEPLOYMENT_NAME_DEFAULT = "kroxylicious-operator";
    private static final String CATALOG_NAMESPACE_DEFAULT = "openshift-marketplace";
    private static final boolean SYNC_RESOURCES_DELETION_DEFAULT = false;
    private static final String ARCHITECTURE_DEFAULT = System.getProperty("os.arch");
    private static final String KROXYLICIOUS_OPERATOR_INSTALL_DIR_DEFAULT = System.getProperty("user.dir") + "/../kroxylicious-operator/target/packaged/install/";
    public static final String CURL_IMAGE_DEFAULT = Constants.DOCKER_REGISTRY_GCR_MIRROR + "/curlimages/curl:8.15.0";

    public static final String KAFKA_VERSION = ENVIRONMENT_VARIABLES.getOrDefault(KAFKA_VERSION_ENV, KAFKA_VERSION_DEFAULT);
    public static final String KROXYLICIOUS_OPERATOR_VERSION = ENVIRONMENT_VARIABLES.getOrDefault(KROXYLICIOUS_OPERATOR_VERSION_ENV, KROXYLICIOUS_VERSION_DEFAULT);

    /**
     * SKIP_TEARDOWN env variable assignment.
     */
    public static final boolean SKIP_TEARDOWN = ENVIRONMENT_VARIABLES.getOrDefault(SKIP_TEARDOWN_ENV, Boolean::parseBoolean, SKIP_TEARDOWN_DEFAULT);

    public static final String CONTAINER_CONFIG_PATH = ENVIRONMENT_VARIABLES.getOrDefault(CONTAINER_CONFIG_PATH_ENV, CONTAINER_CONFIG_PATH_DEFAULT);

    public static final boolean SKIP_STRIMZI_INSTALL = ENVIRONMENT_VARIABLES.getOrDefault(SKIP_STRIMZI_INSTALL_ENV, Boolean::parseBoolean, SKIP_STRIMZI_INSTALL_DEFAULT);

    public static final String KAFKA_CLIENT = ENVIRONMENT_VARIABLES.getOrDefault(KAFKA_CLIENT_ENV, KAFKA_CLIENT_DEFAULT);

    public static final String STRIMZI_VERSION = ENVIRONMENT_VARIABLES.getOrDefault(STRIMZI_VERSION_ENV, STRIMZI_VERSION_DEFAULT);

    public static final String CLUSTER_DUMP_DIR = ENVIRONMENT_VARIABLES.getOrDefault(CLUSTER_DUMP_DIR_ENV, CLUSTER_DUMP_DIR_DEFAULT);

    public static final String STRIMZI_NAMESPACE = ENVIRONMENT_VARIABLES.getOrDefault(STRIMZI_NAMESPACE_ENV, Constants.KAFKA_DEFAULT_NAMESPACE);

    public static final String AWS_ACCESS_KEY_ID = ENVIRONMENT_VARIABLES.getOrDefault(AWS_ACCESS_KEY_ID_ENV, AWS_ACCESS_KEY_ID_DEFAULT);

    public static final String AWS_SECRET_ACCESS_KEY = ENVIRONMENT_VARIABLES.getOrDefault(AWS_SECRET_ACCESS_KEY_ENV, AWS_SECRET_ACCESS_KEY_DEFAULT);

    public static final String AWS_USE_CLOUD = ENVIRONMENT_VARIABLES.getOrDefault(AWS_USE_CLOUD_ENV, AWS_USE_CLOUD_DEFAULT);

    public static final String AWS_KROXYLICIOUS_ACCESS_KEY_ID = ENVIRONMENT_VARIABLES.getOrDefault(AWS_KROXYLICIOUS_ACCESS_KEY_ID_ENV,
            AWS_KROXYLICIOUS_ACCESS_KEY_ID_DEFAULT);

    public static final String AWS_KROXYLICIOUS_SECRET_ACCESS_KEY = ENVIRONMENT_VARIABLES.getOrDefault(AWS_KROXYLICIOUS_SECRET_ACCESS_KEY_ENV,
            AWS_KROXYLICIOUS_SECRET_ACCESS_KEY_DEFAULT);

    public static final String AWS_REGION = ENVIRONMENT_VARIABLES.getOrDefault(AWS_REGION_ENV, AWS_REGION_DEFAULT);

    public static final String KROXYLICIOUS_OPERATOR_BUNDLE_IMAGE = ENVIRONMENT_VARIABLES.getOrDefault(KROXYLICIOUS_OPERATOR_BUNDLE_IMAGE_ENV, "");

    public static final String TEST_CLIENTS_IMAGE = ENVIRONMENT_VARIABLES.getOrDefault(TEST_CLIENTS_IMAGE_ENV, TEST_CLIENTS_IMAGE_DEFAULT);
    public static final String TEST_CLIENTS_PULL_SECRET = ENVIRONMENT_VARIABLES.getOrDefault(TEST_CLIENTS_PULL_SECRET_ENV, "");
    public static final String OLM_OPERATOR_CHANNEL = ENVIRONMENT_VARIABLES.getOrDefault(OLM_OPERATOR_CHANNEL_ENV, OLM_OPERATOR_CHANNEL_DEFAULT);
    public static final String CATALOG_SOURCE_NAME = ENVIRONMENT_VARIABLES.getOrDefault(CATALOG_SOURCE_NAME_ENV, CATALOG_SOURCE_NAME_DEFAULT);
    public static final String CATALOG_NAMESPACE = ENVIRONMENT_VARIABLES.getOrDefault(CATALOG_NAMESPACE_ENV, CATALOG_NAMESPACE_DEFAULT);
    public static final String KROXYLICIOUS_OLM_DEPLOYMENT_NAME = ENVIRONMENT_VARIABLES.getOrDefault(KROXYLICIOUS_OLM_DEPLOYMENT_NAME_ENV,
            KROXYLICIOUS_OLM_DEPLOYMENT_NAME_DEFAULT);
    public static final boolean SYNC_RESOURCES_DELETION = ENVIRONMENT_VARIABLES.getOrDefault(SYNC_RESOURCES_DELETION_ENV, Boolean::parseBoolean,
            SYNC_RESOURCES_DELETION_DEFAULT);
    public static final String ARCHITECTURE = ENVIRONMENT_VARIABLES.getOrDefault(ARCHITECTURE_ENV, ARCHITECTURE_DEFAULT);
    public static final String KROXYLICIOUS_OPERATOR_INSTALL_DIR = ENVIRONMENT_VARIABLES.getOrDefault(KROXYLICIOUS_OPERATOR_INSTALL_DIR_ENV,
            KROXYLICIOUS_OPERATOR_INSTALL_DIR_DEFAULT);
    public static final String CURL_IMAGE = ENVIRONMENT_VARIABLES.getOrDefault(CURL_IMAGE_ENV, CURL_IMAGE_DEFAULT);

    private static String readMetadataProperty(String property) {
        var p = new Properties();
        var metadataProps = "/metadata.properties";
        try (var stream = Environment.class.getResourceAsStream(metadataProps)) {
            Objects.requireNonNull(stream, metadataProps + " is not present on the classpath");
            p.load(stream);
            String version = p.getProperty(property);
            if (version == null) {
                throw new IllegalStateException(property + " key absent in " + metadataProps);
            }
            else if (version.startsWith("$")) {
                throw new IllegalStateException(
                        "likely unexpanded property reference found in '" + version + "', check Maven filtering configuration of resource " + metadataProps);
            }
            return version;
        }
        catch (IOException e) {
            throw new UncheckedIOException("error while streaming " + metadataProps, e);
        }
    }

    private static String determineKroxyliciousVersion() {
        return readMetadataProperty("kroxylicious.version");
    }

    private static String determineKafkaVersion() {
        return readMetadataProperty("kafka.version");
    }

    private static String determineStrimziVersion() {
        return readMetadataProperty("strimzi.version");
    }
}
