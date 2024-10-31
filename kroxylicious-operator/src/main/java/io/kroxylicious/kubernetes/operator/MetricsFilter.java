/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import io.fabric8.kubernetes.api.model.Service;
import io.javaoperatorsdk.operator.processing.event.source.filter.GenericFilter;

public class MetricsFilter implements GenericFilter<Service> {
    @Override
    public boolean accept(Service resource) {
        return resource.getMetadata().getName().startsWith("metrics-");
    }
}
