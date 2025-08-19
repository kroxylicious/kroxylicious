/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.labels.source.simple;

import java.util.Map;
import java.util.Set;

import io.kroxylicious.proxy.labels.Label;
import io.kroxylicious.proxy.labels.LabelledResource;
import io.kroxylicious.proxy.labels.source.LabelSource;
import io.kroxylicious.proxy.labels.source.LabelSourceFactory;
import io.kroxylicious.proxy.labels.source.LabelSourceFactoryContext;
import io.kroxylicious.proxy.labels.source.LabellingContext;
import io.kroxylicious.proxy.plugin.Plugin;

@Plugin(configType = InlineLabelSourceFactory.Config.class)
public class InlineLabelSourceFactory implements LabelSourceFactory<InlineLabelSourceFactory.Config, InlineLabelSourceFactory.Config> {

    @Override
    public Config initialize(LabelSourceFactoryContext context, Config config) {
        return config;
    }

    @Override
    public LabelSource create(Config initializationData) {
        return initializationData;
    }

    public record ResourceLabels(Map<String, Set<Label>> resourceNameToLabels) {

    }

    public record Config(Map<LabelledResource, ResourceLabels> resourceLabels) implements LabelSource {

        @Override
        public Set<Label> labels(LabelledResource resource, String resourceName, LabellingContext context) {
            ResourceLabels labels = resourceLabels.getOrDefault(resource, new ResourceLabels(Map.of()));
            return labels.resourceNameToLabels().getOrDefault(resourceName, Set.of());
        }
    }
}
