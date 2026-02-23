/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteIngress;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.operator.Annotations;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.StaleReferentStatusException;
import io.kroxylicious.kubernetes.operator.model.networking.NetworkingPlanner;
import io.kroxylicious.kubernetes.operator.model.networking.ProxyNetworkingModel;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.DependencyResolver;
import io.kroxylicious.kubernetes.operator.resolver.ProxyResolutionResult;

import static io.kroxylicious.kubernetes.operator.Annotations.readBootstrapServersFrom;

/**
 * Takes a KafkaProxy, resolves all its dependencies, and then computes a ProxyModel
 * which is intended to be a logical abstraction of the resources that should be manifested
 * in kubernetes. Note this is a work-in-progress, so it only models the ingresses currently.
 */
public class ProxyModelBuilder {

    private final DependencyResolver resolver;

    public ProxyModelBuilder(DependencyResolver resolver) {
        Objects.requireNonNull(resolver);
        this.resolver = resolver;
    }

    public ProxyModel build(KafkaProxy primary, Context<KafkaProxy> context) {
        ProxyResolutionResult resolutionResult = resolver.resolveProxyRefs(primary, context);
        if (!resolutionResult.allReferentsHaveFreshStatus()) {
            String resources = resolutionResult.allReferentsWithStaleStatus().map(it -> ResourcesUtil.namespacedSlug(it, primary)).collect(Collectors.joining(","));
            throw new StaleReferentStatusException("Some referent resources have not been reconciled yet: [" + resources + "]. This should be a transient state.");
        }

        List<RouteHostDetails> routeHostDetails = getRoutes(context);

        // to try and produce the most stable allocation of ports we can, we attempt to consider all clusters in the ingress allocation, even those
        // that we know are unacceptable due to unresolved dependencies.
        ProxyNetworkingModel ingressModel = NetworkingPlanner.planNetworking(primary, resolutionResult, routeHostDetails);
        List<ClusterResolutionResult> clustersWithValidIngresses = resolutionResult.allResolutionResultsInClusterNameOrder()
                .filter(clusterResolutionResult -> clusterResolutionResult.allReferentsFullyResolved() && !ResourcesUtil.hasFreshResolvedRefsFalseCondition(
                        clusterResolutionResult.cluster()))
                .filter(result -> ingressModel.clusterIngressModel(result.cluster()).map(i -> i.ingressExceptions().isEmpty()).orElse(false))
                .toList();
        return new ProxyModel(resolutionResult, ingressModel, clustersWithValidIngresses);
    }

    public record RouteHostDetails(
                                   String namespace,
                                   String clusterName,
                                   String ingressName,
                                   String routeFor,
                                   String host) {};

    public List<RouteHostDetails> getRoutes(Context<KafkaProxy> context) {
        Set<Route> routes = context.getSecondaryResources(Route.class);

        List<RouteHostDetails> routeHostDetails = new ArrayList<>();

        for (Route route : routes) {
            Set<Annotations.ClusterIngressBootstrapServers> bootstrapServers = readBootstrapServersFrom(route);
            if (bootstrapServers.isEmpty()) {
                continue;
            }
            var clusterIngressBootstrapServers = bootstrapServers.toArray(new Annotations.ClusterIngressBootstrapServers[0])[0];

            List<RouteIngress> ingress = route.getStatus().getIngress();
            if (ingress.isEmpty()) {
                continue;
            }

            String routeFor = route.getMetadata().getLabels().getOrDefault("route-for", "");
            if (!routeFor.equals("bootstrap") && !routeFor.equals("node")) {
                continue;
            }

            String hostWithSubdomain = ingress.get(0).getHost(); // one-bootstrap.apps-crc.testing
            ArrayList<String> splitHostWithSubdomain = new ArrayList<>(Arrays.asList(hostWithSubdomain.split("\\.")));
            splitHostWithSubdomain.remove(0);
            String hostWithoutSubdomain = String.join(".", splitHostWithSubdomain); // apps-crc.testing

            routeHostDetails.add(new RouteHostDetails(
                    route.getMetadata().getNamespace(),
                    clusterIngressBootstrapServers.clusterName(),
                    clusterIngressBootstrapServers.ingressName(),
                    routeFor,
                    hostWithoutSubdomain));
        }

        return routeHostDetails;
    }

    public static ProxyModelBuilder contextBuilder() {
        return new ProxyModelBuilder(DependencyResolver.create());
    }

}
