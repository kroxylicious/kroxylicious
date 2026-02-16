/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

/**
 * Interface for detecting configuration changes that require cluster operations.
 * Implementations should focus on specific types of changes (cluster models, filters, etc.).
 */
public interface ChangeDetector {

    /**
     * Name of this change detector for logging and debugging.
     */
    String getName();

    /**
     * Detect configuration changes and return structured change information.
     *
     * @param context The configuration context containing old and new configurations
     * @return ChangeResult containing categorized cluster operations
     */
    ChangeResult detectChanges(ConfigurationChangeContext context);
}
