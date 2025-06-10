/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.util.Map;
import java.util.Set;

import io.fabric8.openshift.api.model.Route;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.BulkDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.operator.model.networking.ProxyNetworkingModel;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toByNameMap;

/**
 * Generates the Kube {@code Route}s for a single KafkaProxy.
 */
@KubernetesDependent
public class OpenShiftRouteDependentResource
        extends CRUDKubernetesDependentResource<Route, KafkaProxy>
        implements BulkDependentResource<Route, KafkaProxy> {

    public OpenShiftRouteDependentResource() {
        super(Route.class);
    }

    @Override
    public Map<String, Route> desiredResources(
                                               KafkaProxy primary,
                                               Context<KafkaProxy> context) {
        KafkaProxyContext kafkaProxyContext = KafkaProxyContext.proxyContext(context);
        var model = kafkaProxyContext.model();
        var clusterNetworkingModels = model.clustersWithValidNetworking().stream()
                .map(ClusterResolutionResult::cluster)
                .filter(cluster -> !kafkaProxyContext.isBroken(cluster))
                .flatMap(cluster -> model.networkingModel().clusterIngressModel(cluster).stream())
                .toList();

        var routes = clusterNetworkingModels.stream()
                .flatMap(ProxyNetworkingModel.ClusterNetworkingModel::routes);
        return routes.collect(toByNameMap());
    }

    @Override
    public Map<String, Route> getSecondaryResources(
                                                    KafkaProxy primary,
                                                    Context<KafkaProxy> context) {
        Set<Route> secondaryResources = context.eventSourceRetriever().getEventSourceFor(Route.class)
                .getSecondaryResources(primary);
        return secondaryResources.stream().collect(toByNameMap());
    }
}
