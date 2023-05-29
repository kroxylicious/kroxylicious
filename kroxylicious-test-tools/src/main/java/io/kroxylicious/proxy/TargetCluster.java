/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import com.fasterxml.jackson.annotation.JsonGetter;

import io.sundr.builder.annotations.Buildable;

@Buildable(editableEnabled = false)
public record TargetCluster(@JsonGetter("bootstrap_servers") String bootstrapServers) {
}
