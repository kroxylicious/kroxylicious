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
    private static final String STRIMZI_URL_ENV = "STRIMZI_URL";
    private static final String SKIP_TEARDOWN_ENV = "SKIP_TEARDOWN";
    public static final String STRIMZI_FEATURE_GATES_ENV = "STRIMZI_FEATURE_GATES";
    private static final String CONTAINER_CONFIG_PATH_ENV = "CONTAINER_CONFIG_PATH";
    private static final String SKIP_STRIMZI_INSTALL_ENV = "SKIP_STRIMZI_INSTALL";

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
     * The url where kroxylicious image lives to be downloaded.
     */
    private static final String KROXY_IMAGE_REPO_DEFAULT = "quay.io/kroxylicious/kroxylicious";

    /**
     * The strimzi installation url for kubernetes.
     */
    private static final String STRIMZI_URL_DEFAULT = "https://strimzi.io/install/latest?namespace=" + Constants.KAFKA_DEFAULT_NAMESPACE;
    /**
     * The default value for skipping the teardown locally.
     */
    private static final String SKIP_TEARDOWN_DEFAULT = "false";
    private static final String STRIMZI_FEATURE_GATES_DEFAULT = "";
    private static final String CONTAINER_CONFIG_PATH_DEFAULT = System.getProperty("user.home") + "/.docker/config.json";
    private static final String SKIP_STRIMZI_INSTALL_DEFAULT = "false";

    /**
     * KAFKA_VERSION env variable assignment
     */
    public static final String KAFKA_VERSION = getOrDefault(KAFKA_VERSION_ENV, KAFKA_VERSION_DEFAULT);

    /**
     * KROXY_VERSION env variable assignment
     */
    public static final String KROXY_VERSION = getOrDefault(KROXY_VERSION_ENV, KROXY_VERSION_DEFAULT);
    /**
     * STRIMZI_URL env variable assignment
     */
    public static final String STRIMZI_URL = getOrDefault(STRIMZI_URL_ENV, STRIMZI_URL_DEFAULT);
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
}
