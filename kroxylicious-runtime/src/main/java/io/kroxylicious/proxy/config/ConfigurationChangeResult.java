/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import io.kroxylicious.proxy.internal.ChangeResult;
import io.kroxylicious.proxy.internal.ConfigurationChangeContext;
import io.kroxylicious.proxy.internal.VirtualClusterChangeDetector;

/**
 * Result of a configuration change operation, providing counts of
 * clusters that were modified, added, or removed.
 */
public record ConfigurationChangeResult(
                                        int modifiedCount,
                                        int addedCount,
                                        int removedCount) {

    /**
     * Create a ConfigurationChangeResult from a ConfigurationChangeContext
     * by detecting the changes between old and new configurations.
     */
    public static ConfigurationChangeResult from(ConfigurationChangeContext context) {
        ChangeResult changes = detectChanges(context);
        return new ConfigurationChangeResult(
                changes.clustersToModify().size(),
                changes.clustersToAdd().size(),
                changes.clustersToRemove().size());
    }

    /**
     * Detect changes between old and new configurations.
     */
    private static ChangeResult detectChanges(ConfigurationChangeContext context) {
        VirtualClusterChangeDetector detector = new VirtualClusterChangeDetector();
        return detector.detectChanges(context);
    }

    /**
     * Get total number of clusters affected by the change.
     */
    public int getTotalChanged() {
        return modifiedCount + addedCount + removedCount;
    }

    /**
     * Check if any changes were made.
     */
    public boolean hasChanges() {
        return getTotalChanged() > 0;
    }
}
