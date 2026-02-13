/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.List;

/**
 * Result of change detection containing categorized cluster operations.
 * This record provides clear separation between different types of cluster operations:
 * - Remove: Clusters that should be removed (exist in old but not new config)
 * - Add: Clusters that should be added (exist in new but not old config)
 * - Modify: Clusters that should be modified (exist in both but with different configuration)
 */
public record ChangeResult(
                           List<String> clustersToRemove,
                           List<String> clustersToAdd,
                           List<String> clustersToModify) {

    /**
     * Check if there are any changes detected.
     */
    public boolean hasChanges() {
        return !clustersToRemove.isEmpty() || !clustersToAdd.isEmpty() || !clustersToModify.isEmpty();
    }

    /**
     * Get total number of cluster operations.
     */
    public int getTotalOperations() {
        return clustersToRemove.size() + clustersToAdd.size() + clustersToModify.size();
    }

    /**
     * Get a summary string of the changes.
     */
    public String getSummary() {
        return String.format("Remove: %d, Add: %d, Modify: %d",
                clustersToRemove.size(), clustersToAdd.size(), clustersToModify.size());
    }
}
