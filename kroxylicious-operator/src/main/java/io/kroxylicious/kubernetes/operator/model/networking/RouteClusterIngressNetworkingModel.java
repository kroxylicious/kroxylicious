/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.openshift.api.model.RouteBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.OpenShiftRoute;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.operator.Annotations;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.model.RouteHostDetails;
import io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy.ProxyDeploymentDependentResource;
import io.kroxylicious.proxy.config.NodeIdentificationStrategyFactory;
import io.kroxylicious.proxy.config.SniHostIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.service.HostPort;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static java.lang.Math.toIntExact;

public record RouteClusterIngressNetworkingModel(KafkaProxy proxy,
                                                 VirtualKafkaCluster cluster,
                                                 KafkaProxyIngress ingress,
                                                 OpenShiftRoute openShiftRoute,
                                                 List<NodeIdRanges> nodeIdRanges,
                                                 Tls tls,
                                                 int sharedSniPort,
                                                 List<RouteHostDetails> routeHostDetails)
        implements ClusterIngressNetworkingModel {

    public RouteClusterIngressNetworkingModel {
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(ingress);
        Objects.requireNonNull(openShiftRoute);
        Objects.requireNonNull(nodeIdRanges);
        if (nodeIdRanges.isEmpty()) {
            throw new IllegalArgumentException("nodeIdRanges cannot be empty");
        }
        Objects.requireNonNull(tls);
        Objects.requireNonNull(routeHostDetails);
    }

    public static final int CLIENT_FACING_ROUTE_PORT = 443;

    @Override
    public Stream<ServiceBuilder> services() {
        ObjectMetaBuilder metadataBuilder = baseMetadataBuilder().withName(bootstrapServiceName());
        Annotations.annotateWithBootstrapServers(metadataBuilder,
                Set.of(new Annotations.ClusterIngressBootstrapServers(ResourcesUtil.name(cluster), ResourcesUtil.name(ingress), bootstrapServers())));
        var bootstrapService = createClusterIPService(metadataBuilder);

        return Stream.of(bootstrapService);
    }

    @Override
    public Stream<RouteBuilder> routes() {
        ObjectMetaBuilder metadataBuilder = baseMetadataBuilder();
        Annotations.annotateWithBootstrapServers(metadataBuilder,
                Set.of(new Annotations.ClusterIngressBootstrapServers(ResourcesUtil.name(cluster), ResourcesUtil.name(ingress), bootstrapServers())));
        var bootstrapRoute = createRoute(metadataBuilder, suffixedRouteName("bootstrap"), RouteHostDetails.RouteFor.BOOTSTRAP);

        var nodeRoutes = nodeIdRanges.stream()
                .flatMapToInt(nodeIdRange -> IntStream.rangeClosed(toIntExact(nodeIdRange.getStart()), toIntExact(nodeIdRange.getEnd())))
                .mapToObj(upstreamNodeId -> createRoute(metadataBuilder, suffixedRouteName(String.valueOf(upstreamNodeId)), RouteHostDetails.RouteFor.NODE));

        return Stream.concat(Stream.of(bootstrapRoute), nodeRoutes);
    }

    private ServiceBuilder createClusterIPService(ObjectMetaBuilder metadataBuilder) {
        return new ServiceBuilder()
                .withMetadata(metadataBuilder.build())
                .withNewSpec()
                .withSelector(ProxyDeploymentDependentResource.podLabels(proxy))
                .withPorts(new ServicePortBuilder().withName(ResourcesUtil.name(cluster) + "-" + sharedSniPort).withProtocol("TCP").withPort(sharedSniPort)
                        .withTargetPort(new IntOrString(sharedSniPort)).build())
                .endSpec();
    }

    private RouteBuilder createRoute(ObjectMetaBuilder metadataBuilder, String subdomain, RouteHostDetails.RouteFor routeFor) {
        // @formatter:off
        return new RouteBuilder()
                .withMetadata(metadataBuilder.build())
                .editMetadata()
                    .withName(subdomain)
                    .addToLabels(RouteHostDetails.RouteFor.LABEL_KEY, String.valueOf(routeFor))
                .endMetadata()
                .withNewSpec()
                    .withSubdomain(subdomain)
                    .withNewPort()
                    .withNewTargetPort(sharedSniPort)
                .endPort()
                .withNewTls()
                    .withTermination("passthrough")
                .endTls()
                .withNewTo()
                    .withKind("Service")
                    .withName(bootstrapServiceName())
                .endTo()
                .endSpec();
        // @formatter:on
    }

    private ObjectMetaBuilder baseMetadataBuilder() {
        return new ObjectMetaBuilder()
                .withNamespace(namespace(cluster))
                .addToLabels(standardLabels(proxy))
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(proxy)).endOwnerReference()
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(cluster)).endOwnerReference()
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(ingress)).endOwnerReference();
    }

    @Override
    public Stream<ContainerPort> identifyingProxyContainerPorts() {
        return Stream.empty();
    }

    public Optional<String> getDomain(RouteHostDetails.RouteFor routeFor) {
        return RouteHostDetails.findFirstRouteHostWithoutSubdomainFromList(routeHostDetails, namespace(cluster), name(cluster), name(ingress), routeFor);
    }

    @Override
    public NodeIdentificationStrategyFactory nodeIdentificationStrategy() {
        String hostToken = "$(host)";

        HostPort bootstrapAddress = new HostPort("$(virtualClusterName)-bootstrap." + getDomain(RouteHostDetails.RouteFor.BOOTSTRAP).orElse(hostToken), sharedSniPort);
        HostPort advertisedBrokerAddressPattern = new HostPort("$(virtualClusterName)-$(nodeId)." + getDomain(RouteHostDetails.RouteFor.NODE).orElse(hostToken),
                CLIENT_FACING_ROUTE_PORT);
        return new SniHostIdentifiesNodeIdentificationStrategy(bootstrapAddress.toString(),
                advertisedBrokerAddressPattern.toString());
    }

    @Override
    public Optional<Tls> downstreamTls() {
        return Optional.of(tls);
    }

    @Override
    public boolean requiresSharedSniContainerPort() {
        return true;
    }

    private String bootstrapServers() {
        return ResourcesUtil.name(cluster) + "-bootstrap." + getDomain(RouteHostDetails.RouteFor.BOOTSTRAP).orElse("$(host)") + ":" + sharedSniPort;
    }

    private String suffixedRouteName(String suffix) {
        return name(cluster) + "-" + suffix;
    }

    private String bootstrapServiceName() {
        return name(cluster) + "-" + name(ingress) + "-" + "service";
    }
}
