/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import io.sundr.builder.annotations.Buildable;

@Buildable(editableEnabled = false)
public record VirtualCluster(TargetCluster targetCluster, ClusterEndpointConfigProvider clusterEndpointConfigProvider, String keyStoreFile, String keyPassword, boolean logNetwork, boolean logFrames) {
}
