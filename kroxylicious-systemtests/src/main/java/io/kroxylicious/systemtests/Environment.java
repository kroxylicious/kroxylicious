/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.util.function.Function;

/**
 * The type Environment.
 */
public class Environment {
    /**
     * Env. variables names
     */
    private static final String KAFKA_VERSION_ENV = "KAFKA_VERSION";
    private static final String QUAY_ORG_ENV = "QUAY_ORG";
    private static final String KROXY_VERSION_ENV = "KROXY_VERSION";
    private static final String KROXY_IMAGE_REPO_ENV = "KROXY_IMAGE_REPO";
    private static final String STRIMZI_URL_ENV = "STRIMZI_URL";

    /**
     * The kafka version default value
     */
    public static final String KAFKA_VERSION_DEFAULT = "3.6.0";
    /**
     * The quay org default value
     */
    public static final String QUAY_ORG_DEFAULT = "kroxylicious";

    /**
     * The kroxy version default value
     */
    public static final String KROXY_VERSION_DEFAULT = "0.4.0-SNAPSHOT";
    /**
     * The url where kroxylicious image lives to be downloaded.
     */
    public static final String KROXY_IMAGE_REPO_DEFAULT = "quay.io/kroxylicious/kroxylicious-developer";

    /**
     * The strimzi installation url for kubernetes.
     */
    public static final String STRIMZI_URL_DEFAULT = "https://strimzi.io/install/latest?namespace=" + Constants.KROXY_DEFAULT_NAMESPACE;

    /**
     * KAFKA_VERSION env variable assignment
     */
    public static final String KAFKA_VERSION = getOrDefault(KAFKA_VERSION_ENV, KAFKA_VERSION_DEFAULT);
    /**
     * QUAY_ORG env variable assignment
     */
    public static final String QUAY_ORG = getOrDefault(QUAY_ORG_ENV, QUAY_ORG_DEFAULT);
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

    private static String getOrDefault(String varName, String defaultValue) {
        return getOrDefault(varName, String::toString, defaultValue);
    }

    private static <T> T getOrDefault(String varName, Function<String, T> converter, T defaultValue) {
        return System.getenv(varName) != null ? converter.apply(System.getenv(varName)) : defaultValue;
    }
}
