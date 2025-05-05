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
    private static final String INSTALL_TYPE_ENV = "INSTALL_TYPE";
    public static final InstallType INSTALL_TYPE = ENVIRONMENT_VARIABLES.getOrDefault(INSTALL_TYPE_ENV, InstallType::fromString, InstallType.Yaml);

    /**
     * Env. variables names
     */
    private static final String KAFKA_VERSION_ENV = "KAFKA_VERSION";
    private static final String KROXYLICIOUS_OPERATOR_IMAGE_ENV = "KROXYLICIOUS_OPERATOR_IMAGE_NAME";
    private static final String KROXYLICIOUS_IMAGE_ENV = "KROXYLICIOUS_IMAGE_NAME";
    private static final String KROXYLICIOUS_ORG_ENV = "KROXYLICIOUS_ORG";
    private static final String KROXYLICIOUS_OPERATOR_ORG_ENV = "KROXYLICIOUS_OPERATOR_ORG";
    private static final String KROXYLICIOUS_REGISTRY_ENV = "KROXYLICIOUS_REGISTRY";
    private static final String KROXYLICIOUS_OPERATOR_REGISTRY_ENV = "KROXYLICIOUS_OPERATOR_REGISTRY";
    private static final String KROXYLICIOUS_VERSION_ENV = "KROXYLICIOUS_VERSION";
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
     * The url where kroxylicious image lives to be downloaded.
     */
    private static final String KROXYLICIOUS_IMAGE_REPO_DEFAULT = "quay.io/kroxylicious/kroxylicious";
    private static final String KROXYLICIOUS_OPERATOR_IMAGE_REPO_DEFAULT = "quay.io/kroxylicious/operator";

    /**
     * The default value for skipping the teardown locally.
     */
    private static final boolean SKIP_TEARDOWN_DEFAULT = false;
    public static final String KROXYLICIOUS_IMAGE_DEFAULT = KROXYLICIOUS_IMAGE_REPO_DEFAULT.split("/")[2];
    public static final String KROXYLICIOUS_OPERATOR_IMAGE_DEFAULT = KROXYLICIOUS_OPERATOR_IMAGE_REPO_DEFAULT.split("/")[2];
    public static final String KROXYLICIOUS_ORG_DEFAULT = KROXYLICIOUS_IMAGE_REPO_DEFAULT.split("/")[1];
    public static final String KROXYLICIOUS_OPERATOR_ORG_DEFAULT = KROXYLICIOUS_OPERATOR_IMAGE_REPO_DEFAULT.split("/")[1];
    public static final String KROXYLICIOUS_REGISTRY_DEFAULT = KROXYLICIOUS_IMAGE_REPO_DEFAULT.split("/")[0];
    public static final String KROXYLICIOUS_OPERATOR_REGISTRY_DEFAULT = KROXYLICIOUS_OPERATOR_IMAGE_REPO_DEFAULT.split("/")[0];
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

    /**
     * KAFKA_VERSION env variable assignment
     */
    public static final String KAFKA_VERSION = ENVIRONMENT_VARIABLES.getOrDefault(KAFKA_VERSION_ENV, KAFKA_VERSION_DEFAULT);

    /**
     * KROXYLICIOUS_VERSION env variable assignment
     */
    public static final String KROXYLICIOUS_VERSION = ENVIRONMENT_VARIABLES.getOrDefault(KROXYLICIOUS_VERSION_ENV, KROXYLICIOUS_VERSION_DEFAULT);
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

    public static final String KROXYLICIOUS_IMAGE = ENVIRONMENT_VARIABLES.getOrDefault(KROXYLICIOUS_IMAGE_ENV, KROXYLICIOUS_IMAGE_DEFAULT);
    public static final String KROXYLICIOUS_OPERATOR_IMAGE = ENVIRONMENT_VARIABLES.getOrDefault(KROXYLICIOUS_OPERATOR_IMAGE_ENV, KROXYLICIOUS_OPERATOR_IMAGE_DEFAULT);
    public static final String KROXYLICIOUS_ORG = ENVIRONMENT_VARIABLES.getOrDefault(KROXYLICIOUS_ORG_ENV, KROXYLICIOUS_ORG_DEFAULT);
    public static final String KROXYLICIOUS_OPERATOR_ORG = ENVIRONMENT_VARIABLES.getOrDefault(KROXYLICIOUS_OPERATOR_ORG_ENV, KROXYLICIOUS_OPERATOR_ORG_DEFAULT);
    public static final String KROXYLICIOUS_REGISTRY = ENVIRONMENT_VARIABLES.getOrDefault(KROXYLICIOUS_REGISTRY_ENV, KROXYLICIOUS_REGISTRY_DEFAULT);
    public static final String KROXYLICIOUS_OPERATOR_REGISTRY = ENVIRONMENT_VARIABLES.getOrDefault(KROXYLICIOUS_OPERATOR_REGISTRY_ENV,
            KROXYLICIOUS_OPERATOR_REGISTRY_DEFAULT);

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
