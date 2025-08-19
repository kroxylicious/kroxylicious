/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.model;

import java.util.Set;

import io.kroxylicious.proxy.labels.Label;
import io.kroxylicious.proxy.labels.LabelledResource;
import io.kroxylicious.proxy.labels.source.LabelSource;
import io.kroxylicious.proxy.labels.source.LabelSourceFactory;
import io.kroxylicious.proxy.labels.source.LabellingContext;

public record InitializedLabelSource(LabelSourceFactory<?, ?> labelSourceFactory, LabelSource source) implements LabelSource {

    @Override
    public Set<Label> labels(LabelledResource resource, String resourceName, LabellingContext context) {
        return source.labels(resource, resourceName, context);
    }

}
