/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.openshift.api.model.Route;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.BulkDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.model.networking.ProxyNetworkingModel;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.toByNameMap;

/**
 * Generates the OpenShift {@code Route} for a single virtual cluster.
 */
@KubernetesDependent
public class ClusterRouteDependentResource
        extends CRUDKubernetesDependentResource<Route, KafkaProxy>
        implements BulkDependentResource<Route, KafkaProxy> {

    public ClusterRouteDependentResource() {
        super(Route.class);
    }

    /**
     * @return The {@code metadata.name} of the desired {@code Route}.
     */
    static String routeName(VirtualKafkaCluster cluster) {
        Objects.requireNonNull(cluster);
        return ResourcesUtil.name(cluster);
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

        var routeStream = clusterNetworkingModels.stream()
                .flatMap(ProxyNetworkingModel.ClusterNetworkingModel::routes);

        return routeStream.collect(toByNameMap());
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
