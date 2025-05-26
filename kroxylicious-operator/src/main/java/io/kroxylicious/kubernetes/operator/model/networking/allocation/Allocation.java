/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking.allocation;

import io.kroxylicious.kubernetes.operator.model.networking.PortRange;

public record Allocation(String clusterName, String ingressName, PortRange range) {}
