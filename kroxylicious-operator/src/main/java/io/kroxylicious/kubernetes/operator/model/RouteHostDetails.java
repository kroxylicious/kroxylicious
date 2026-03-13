/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteIngress;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.Annotations;
import io.kroxylicious.kubernetes.operator.model.networking.RouteClusterIngressNetworkingModel;

import static io.kroxylicious.kubernetes.operator.Annotations.readBootstrapServersFrom;

/**
 * Record used for efficiently storing identifying information about a {@link Route}, along with its subdomain-less host.
 * Used to substitute a resolved {@link RouteIngress#getHost()} host in to {@link RouteClusterIngressNetworkingModel#nodeIdentificationStrategy()}.
 *
 * @param routeNamespace Namespace of the {@link Route}
 * @param clusterName Name of the {@link VirtualKafkaCluster}
 * @param ingressName Name of the {@link KafkaProxyIngress}
 * @param routeFor Value of the {@link RouteFor} label
 * @param hostWithoutSubdomain The host of the {@link Route} without the subdomain e.g. if {@link RouteIngress#getHost()} returns
 * {@code $(virtualClusterName)-bootstrap.apps-crc.testing} then this value is {@code apps-crc.testing}
 */
public record RouteHostDetails(
                               String routeNamespace,
                               String clusterName,
                               String ingressName,
                               RouteFor routeFor,
                               String hostWithoutSubdomain) {

    /**
     * Label used to identify a {@link Route} target.
     */
    public enum RouteFor {
        BOOTSTRAP,
        NODE;

        public static final String LABEL_KEY = "route-for";

        @Override
        public String toString() {
            return super.toString().toLowerCase();
        }
    }

    /**
     * Checks if the {@link Route} matches the provided details. Used for finding the {@link RouteHostDetails#hostWithoutSubdomain()}.
     */
    private boolean isRouteMatching(String routeNamespace, String clusterName, String ingressName, RouteFor routeFor) {
        return this.routeNamespace.equals(routeNamespace)
                && this.clusterName.equals(clusterName)
                && this.ingressName.equals(ingressName)
                && this.routeFor.equals(routeFor);
    }

    /**
     * Tries to find the first {@link RouteHostDetails} that matches the provided details and return its {@link RouteHostDetails#hostWithoutSubdomain()}.
     * <p>
     * See {@link RouteHostDetails} for explanation of parameters.
     *
     * @return An {@link Optional} containing the {@link RouteHostDetails#hostWithoutSubdomain()} if found, otherwise {@link Optional#empty()}.
     */
    public static Optional<String> findFirstRouteHostWithoutSubdomainFromList(List<RouteHostDetails> routeHostDetailsList, String routeNamespace, String clusterName,
                                                                              String ingressName, RouteHostDetails.RouteFor routeFor) {
        List<RouteHostDetails> details = routeHostDetailsList.stream()
                .filter(r -> r.isRouteMatching(routeNamespace, clusterName, ingressName, routeFor))
                .toList();

        if (details.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(details.get(0).hostWithoutSubdomain());
    }

    /**
     * Fetches {@link Route} secondary resources and maps them to a {@link RouteHostDetails} list.
     */
    @SuppressWarnings("java:S135") // Readability
    public static List<RouteHostDetails> fetchRouteHostDetailsList(Context<KafkaProxy> context) {
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

            RouteFor routeFor;
            try {
                String routeForLabelValue = route.getMetadata().getLabels().get(RouteFor.LABEL_KEY);
                routeFor = RouteFor.valueOf(routeForLabelValue.toUpperCase());
            }
            catch (Exception e) {
                continue;
            }

            String hostWithSubdomain = ingress.get(0).getHost(); // one-bootstrap.apps-crc.testing
            ArrayList<String> splitHostWithSubdomain = new ArrayList<>(Arrays.asList(hostWithSubdomain.split("\\.")));
            if (splitHostWithSubdomain.size() < 2) {
                continue;
            }
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
}
