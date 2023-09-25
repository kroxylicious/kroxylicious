/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious;

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
    public static final String QUAY_ORG_ENV = "QUAY_ORG";

    /**
     * Env. variables defaults
     */
    public static final String KAFKA_VERSION_DEFAULT = "3.5.1";
    public static final String QUAY_ORG_DEFAULT = "kroxylicious";

    /**
     * Env. variables assignment
     */
    public static final String KAFKA_VERSION = getOrDefault(KAFKA_VERSION_ENV, KAFKA_VERSION_DEFAULT);
    public static final String QUAY_ORG = getOrDefault(QUAY_ORG_ENV, QUAY_ORG_DEFAULT);

    private static String getOrDefault(String varName, String defaultValue) {
        return getOrDefault(varName, String::toString, defaultValue);
    }

    private static <T> T getOrDefault(String var, Function<String, T> converter, T defaultValue) {
        T returnValue = System.getenv(var) != null ? converter.apply(System.getenv(var)) : defaultValue;

        VALUES.put(var, String.valueOf(returnValue));
        return returnValue;
    }
}
