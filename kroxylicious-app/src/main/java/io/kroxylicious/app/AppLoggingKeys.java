/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.app;

/**
 * Common keys for structured logging in kroxylicious-app.
 */
public class AppLoggingKeys {

    private AppLoggingKeys() {
    }

    /**
     * The java vendor.
     */
    public static final String JAVA_VENDOR = "javaVendor";

    /**
     * The java version.
     */
    public static final String JAVA_VERSION = "javaVersion";

    /**
     * The os arch.
     */
    public static final String OS_ARCH = "osArch";

    /**
     * The os name.
     */
    public static final String OS_NAME = "osName";

    /**
     * The os version.
     */
    public static final String OS_VERSION = "osVersion";

    /**
     * The version.
     */
    public static final String VERSION = "version";

}
