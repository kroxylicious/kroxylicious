/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.labels.source;

import java.util.Set;

import io.kroxylicious.proxy.labels.Label;
import io.kroxylicious.proxy.labels.LabelledResource;

public interface LabelSource {
    Set<Label> labels(LabelledResource resource, String resourceName, LabellingContext context);

    // real service should be async
    default boolean hasAnyOf(LabelledResource resource, String resourceName, Set<Label> labels, LabellingContext context) {
        var allLabels = labels(resource, resourceName, context);
        return labels.stream().anyMatch(allLabels::contains);
    }

}
