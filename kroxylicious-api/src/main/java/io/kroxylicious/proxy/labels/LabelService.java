/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.labels;

import java.util.Set;

public interface LabelService {
    Set<Label> labels(LabelledResource resource, String resourceName);

    // real service should be async
    default boolean hasAnyOf(LabelledResource resource, String resourceName, Set<Label> labels) {
        var allLabels = labels(resource, resourceName);
        return labels.stream().anyMatch(allLabels::contains);
    }

}
