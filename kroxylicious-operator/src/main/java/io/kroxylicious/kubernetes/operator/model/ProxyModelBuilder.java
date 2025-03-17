/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.model.ingress.IngressAllocator;
import io.kroxylicious.kubernetes.operator.model.ingress.ProxyIngressModel;
import io.kroxylicious.kubernetes.operator.resolver.ClusterConditionUnresolvedDependencyReporter;
import io.kroxylicious.kubernetes.operator.resolver.DependencyResolver;
import io.kroxylicious.kubernetes.operator.resolver.DependencyResolverImpl;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.UnresolvedDependencyReporter;

/**
 * Takes a KafkaProxy, resolves all its dependencies, and then computes a ProxyModel
 * which is intended to be a logical abstraction of the resources that should be manifested
 * in kubernetes. Note this is a work-in-progress, so it only models the ingresses currently.
 */
public class ProxyModelBuilder {

    private final DependencyResolver resolver;
    private final UnresolvedDependencyReporter reporter;

    public ProxyModelBuilder(DependencyResolver resolver, UnresolvedDependencyReporter reporter) {
        Objects.requireNonNull(resolver);
        Objects.requireNonNull(reporter);
        this.resolver = resolver;
        this.reporter = reporter;
    }

    public ProxyModel build(KafkaProxy primary, Context<KafkaProxy> context) {
        ResolutionResult resolutionResult = resolver.deepResolve(context, reporter);
        Set<KafkaProxyIngress> ingresses = resolutionResult.ingresses();
        // to try and produce the most stable allocation of ports we can, we attempt to consider all clusters in the ingress allocation, even those
        // that we know are unacceptable due to unresolved dependencies.
        List<VirtualKafkaCluster> allClusters = resolutionResult.allClustersInNameOrder();
        ProxyIngressModel ingressModel = IngressAllocator.allocateProxyIngressModel(primary, context, resolutionResult);
        List<VirtualKafkaCluster> clustersWithValidIngresses = resolutionResult.fullyResolvedClustersInNameOrder().stream()
                .filter(cluster -> ingressModel.clusterIngressModel(cluster).map(i -> i.ingressExceptions().isEmpty()).orElse(false)).toList();
        return new ProxyModel(resolutionResult, ingressModel, clustersWithValidIngresses);
    }

    public static ProxyModelBuilder contextBuilder(Context<KafkaProxy> context) {
        return new ProxyModelBuilder(DependencyResolverImpl.create(), new ClusterConditionUnresolvedDependencyReporter(context));
    }

}
