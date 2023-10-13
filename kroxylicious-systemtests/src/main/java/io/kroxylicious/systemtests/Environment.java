/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * The type Environment.
 */
public class Environment {

    private static final Map<String, String> VALUES = new HashMap<>();

    /**
     * Env. variables names
     */
    public static final String KAFKA_VERSION_ENV = "KAFKA_VERSION";
    /**
     * The constant QUAY_ORG_ENV.
     */
    public static final String QUAY_ORG_ENV = "QUAY_ORG";
    /**
     * The constant KROXY_VERSION_ENV.
     */
    public static final String KROXY_VERSION_ENV = "KROXY_VERSION";

    /**
     * Env. variables defaults
     */
    public static final String KAFKA_VERSION_DEFAULT = "3.5.1";
    /**
     * The constant QUAY_ORG_DEFAULT.
     */
    public static final String QUAY_ORG_DEFAULT = "kroxylicious";

    /**
     * The constant KROXY_VERSION_DEFAULT.
     */
    public static final String KROXY_VERSION_DEFAULT = "0.3.0-SNAPSHOT";

    /**
     * Env. variables assignment
     */
    public static final String KAFKA_VERSION = getOrDefault(KAFKA_VERSION_ENV, KAFKA_VERSION_DEFAULT);
    /**
     * The constant QUAY_ORG.
     */
    public static final String QUAY_ORG = getOrDefault(QUAY_ORG_ENV, QUAY_ORG_DEFAULT);
    /**
     * The constant KROXY_VERSION.
     */
    public static final String KROXY_VERSION = getOrDefault(KROXY_VERSION_ENV, KROXY_VERSION_DEFAULT);

    private static String getOrDefault(String varName, String defaultValue) {
        return getOrDefault(varName, String::toString, defaultValue);
    }

    private static <T> T getOrDefault(String varName, Function<String, T> converter, T defaultValue) {
        T returnValue = System.getenv(varName) != null ? converter.apply(System.getenv(varName)) : defaultValue;

        VALUES.put(varName, String.valueOf(returnValue));
        return returnValue;
    }
}
