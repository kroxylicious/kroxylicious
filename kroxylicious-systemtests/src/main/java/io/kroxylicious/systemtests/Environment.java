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
import java.util.function.Function;

/**
 * The type Environment.
 */
public class Environment {

    private Environment() {
    }

    /**
     * Env. variables names
     */
    private static final String KAFKA_VERSION_ENV = "KAFKA_VERSION";
    private static final String KROXY_VERSION_ENV = "KROXYLICIOUS_VERSION";
    private static final String KROXY_IMAGE_REPO_ENV = "KROXYLICIOUS_IMAGE_REPO";
    private static final String SKIP_TEARDOWN_ENV = "SKIP_TEARDOWN";
    public static final String STRIMZI_FEATURE_GATES_ENV = "STRIMZI_FEATURE_GATES";
    private static final String CONTAINER_CONFIG_PATH_ENV = "CONTAINER_CONFIG_PATH";
    private static final String VAULT_CHART_VERSION_ENV = "VAULT_CHART_VERSION";
    private static final String AWS_LOCALSTACK_CHART_VERSION_ENV = "AWS_LOCALSTACK_CHART_VERSION";
    private static final String SKIP_STRIMZI_INSTALL_ENV = "SKIP_STRIMZI_INSTALL";
    private static final String KAFKA_CLIENT_ENV = "KAFKA_CLIENT";
    private static final String STRIMZI_VERSION_ENV = "STRIMZI_VERSION";
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
    private static final String KROXY_VERSION_DEFAULT;

    static {
        KROXY_VERSION_DEFAULT = determineKroxyliciousVersion();
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
    private static final String KROXY_IMAGE_REPO_DEFAULT = "quay.io/kroxylicious/kroxylicious";

    /**
     * The default value for skipping the teardown locally.
     */
    private static final String SKIP_TEARDOWN_DEFAULT = "false";
    private static final String STRIMZI_FEATURE_GATES_DEFAULT = "";
    private static final String CONTAINER_CONFIG_PATH_DEFAULT = System.getProperty("user.home") + "/.docker/config.json";
    private static final String VAULT_CHART_VERSION_DEFAULT = "0.27.0";
    private static final String AWS_LOCALSTACK_CHART_VERSION_DEFAULT = "0.6.20";
    private static final String SKIP_STRIMZI_INSTALL_DEFAULT = "false";
    private static final String KAFKA_CLIENT_DEFAULT = "strimzi_test_client";
    private static final String CLUSTER_DUMP_DIR_DEFAULT = System.getProperty("java.io.tmpdir");
    public static final String AWS_ACCESS_KEY_ID_DEFAULT = "test";
    private static final String AWS_SECRET_ACCESS_KEY_DEFAULT = "test";
    private static final String AWS_USE_CLOUD_DEFAULT = "false";
    public static final String AWS_KROXYLICIOUS_ACCESS_KEY_ID_DEFAULT = AWS_ACCESS_KEY_ID_DEFAULT;
    private static final String AWS_KROXYLICIOUS_SECRET_ACCESS_KEY_DEFAULT = AWS_SECRET_ACCESS_KEY_DEFAULT;
    public static final String AWS_REGION_DEFAULT = "us-east-2";

    /**
     * KAFKA_VERSION env variable assignment
     */
    public static final String KAFKA_VERSION = getOrDefault(KAFKA_VERSION_ENV, KAFKA_VERSION_DEFAULT);

    /**
     * KROXY_VERSION env variable assignment
     */
    public static final String KROXY_VERSION = getOrDefault(KROXY_VERSION_ENV, KROXY_VERSION_DEFAULT);

    /**
     * KROXY_IMAGE_REPO env variable assignment
     */
    public static final String KROXY_IMAGE_REPO = getOrDefault(KROXY_IMAGE_REPO_ENV, KROXY_IMAGE_REPO_DEFAULT);
    /**
     * SKIP_TEARDOWN env variable assignment.
     */
    public static final boolean SKIP_TEARDOWN = Boolean.parseBoolean(getOrDefault(SKIP_TEARDOWN_ENV, SKIP_TEARDOWN_DEFAULT));

    public static final String STRIMZI_FEATURE_GATES = getOrDefault(STRIMZI_FEATURE_GATES_ENV, STRIMZI_FEATURE_GATES_DEFAULT);

    public static final String CONTAINER_CONFIG_PATH = getOrDefault(CONTAINER_CONFIG_PATH_ENV, CONTAINER_CONFIG_PATH_DEFAULT);

    public static final boolean SKIP_STRIMZI_INSTALL = Boolean.parseBoolean(getOrDefault(SKIP_STRIMZI_INSTALL_ENV, SKIP_STRIMZI_INSTALL_DEFAULT));

    public static final String VAULT_CHART_VERSION = getOrDefault(VAULT_CHART_VERSION_ENV, VAULT_CHART_VERSION_DEFAULT);

    public static final String AWS_LOCALSTACK_CHART_VERSION = getOrDefault(AWS_LOCALSTACK_CHART_VERSION_ENV, AWS_LOCALSTACK_CHART_VERSION_DEFAULT);

    public static final String KAFKA_CLIENT = getOrDefault(KAFKA_CLIENT_ENV, KAFKA_CLIENT_DEFAULT);

    public static final String STRIMZI_VERSION = getOrDefault(STRIMZI_VERSION_ENV, STRIMZI_VERSION_DEFAULT);

    public static final String CLUSTER_DUMP_DIR = getOrDefault(CLUSTER_DUMP_DIR_ENV, CLUSTER_DUMP_DIR_DEFAULT);

    public static final String AWS_ACCESS_KEY_ID = getOrDefault(AWS_ACCESS_KEY_ID_ENV, AWS_ACCESS_KEY_ID_DEFAULT);

    public static final String AWS_SECRET_ACCESS_KEY = getOrDefault(AWS_SECRET_ACCESS_KEY_ENV, AWS_SECRET_ACCESS_KEY_DEFAULT);

    public static final String AWS_USE_CLOUD = getOrDefault(AWS_USE_CLOUD_ENV, AWS_USE_CLOUD_DEFAULT);

    public static final String AWS_KROXYLICIOUS_ACCESS_KEY_ID = getOrDefault(AWS_KROXYLICIOUS_ACCESS_KEY_ID_ENV, AWS_KROXYLICIOUS_ACCESS_KEY_ID_DEFAULT);

    public static final String AWS_KROXYLICIOUS_SECRET_ACCESS_KEY = getOrDefault(AWS_KROXYLICIOUS_SECRET_ACCESS_KEY_ENV, AWS_KROXYLICIOUS_SECRET_ACCESS_KEY_DEFAULT);

    public static final String AWS_REGION = getOrDefault(AWS_REGION_ENV, AWS_REGION_DEFAULT);

    private static String getOrDefault(String varName, String defaultValue) {
        return getOrDefault(varName, String::toString, defaultValue);
    }

    private static <T> T getOrDefault(String varName, Function<String, T> converter, T defaultValue) {
        return System.getenv(varName) != null ? converter.apply(System.getenv(varName)) : defaultValue;
    }

    private static String readMetadataProperty(String property) {
        var p = new Properties();
        var metadataProps = "/metadata.properties";
        try (var stream = Environment.class.getResourceAsStream(metadataProps)) {
            Objects.requireNonNull(stream, metadataProps + " is not present on the classpath");
            p.load(stream);
            var version = p.getProperty(property);
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
