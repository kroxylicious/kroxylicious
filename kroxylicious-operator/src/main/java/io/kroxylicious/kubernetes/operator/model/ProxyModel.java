/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model;

import java.util.List;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.model.ingress.ProxyIngressModel;
import io.kroxylicious.kubernetes.operator.resolver.ProxyResolutionResult;

/**
 * This class aims to encapsulate the logical model of what we want to manifest (it is a work in progress).
 * During a reconciliation we take the raw Custom Resources, resolve the graph of references between them
 * and allocate shared resources, like container ports to various components.
 * @param resolutionResult the resolved dependencies for this reconciliation
 * @param ingressModel the ingress model for this reconciliation
 * @param clustersWithValidIngresses virtual kafka clusters that are fully resolved and have valid ingresses
 */
public record ProxyModel(ProxyResolutionResult resolutionResult,
                         ProxyIngressModel ingressModel,
                         List<VirtualKafkaCluster> clustersWithValidIngresses) {

}
