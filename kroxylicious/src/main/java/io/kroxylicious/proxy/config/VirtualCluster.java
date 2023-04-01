/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class VirtualCluster {

    private final TargetCluster targetCluster;

    public VirtualCluster(@JsonProperty(value = "targetCluster") TargetCluster targetCluster) {
        this.targetCluster = targetCluster;
    }

    public TargetCluster targetCluster() {
        return targetCluster;
    }

    @Override
    public String toString() {
        return "VirtualCluster [targetCluster=" + targetCluster + "]";
    }
}
