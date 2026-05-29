/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

/**
 * Exception thrown when a plugin version referenced in configuration does not match
 * the version declared by the plugin implementation via {@link Version} annotation.
 * <p>
 * This can occur when:
 * <ul>
 * <li>Configuration specifies a version that differs from the plugin's {@code @Version} annotation</li>
 * <li>Configuration specifies a version but the plugin has no {@code @Version} annotation</li>
 * <li>Plugin has a {@code @Version} annotation but configuration omits the version (when enforcement is enabled)</li>
 * </ul>
 *
 * @see Version
 */
public class VersionMismatchException extends RuntimeException {

    /**
     * Constructs a new version mismatch exception with the specified detail message.
     *
     * @param message the detail message
     */
    public VersionMismatchException(String message) {
        super(message);
    }
}
