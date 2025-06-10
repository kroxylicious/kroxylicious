/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.openshift.api.model.RouteBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.OpenShiftRoutes;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.operator.Annotations;
import io.kroxylicious.kubernetes.operator.ClusterServiceDependentResource;
import io.kroxylicious.kubernetes.operator.Labels;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.proxy.config.NodeIdentificationStrategy;
import io.kroxylicious.proxy.config.SniHostIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

public record OpenShiftRoutesClusterIngressNetworkingModel(VirtualKafkaCluster cluster,
                                                           KafkaProxyIngress ingress,
                                                           KafkaProxy proxy,
                                                           OpenShiftRoutes openShiftRoutes,
                                                           java.util.List<NodeIdRanges> nodeIdRanges,
                                                           Tls tls,
                                                           int sharedSniPort)
        implements ClusterIngressNetworkingModel, SharedLoadBalancerServiceRequirements {

    public OpenShiftRoutesClusterIngressNetworkingModel {
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(ingress);
        Objects.requireNonNull(proxy);
        Objects.requireNonNull(nodeIdRanges);
        Objects.requireNonNull(openShiftRoutes);
        Objects.requireNonNull(tls);
    }

    private static final int ROUTE_TLS_PORT = 443;

    public static final int DEFAULT_CLIENT_FACING_LOADBALANCER_PORT = 9083;

    @Override
    public Stream<ServiceBuilder> services() {
        return Stream.empty();
    }

    @Override
    public Stream<ContainerPort> identifyingProxyContainerPorts() {
        return Stream.empty();
    }

    @Override
    public NodeIdentificationStrategy nodeIdentificationStrategy() {
        String bootstrap = bootstrapServersHost();
        String advertisedBrokerAddressPattern = prefixedClusterIngressHost("broker-$(nodeId)");
        return new SniHostIdentifiesNodeIdentificationStrategy(HostPort.asString(bootstrap, sharedSniPort),
                HostPort.asString(advertisedBrokerAddressPattern, ROUTE_TLS_PORT));
    }

    public String prefixedClusterIngressHost(String prefix) {
        String baseDomain = name(cluster) + "-" + name(ingress) + "." + openShiftRoutes.getIngressDomain();
        return prefix + "." + baseDomain;
    }

    @Override
    public Stream<RouteBuilder> routes() {
        RouteBuilder bootstrapRoute = routeBuilder(name(cluster) + "-" + name(ingress) + "-bootstrap", bootstrapServersHost());
        IntStream upstreamNodeIds = nodeIdRanges.stream()
                .flatMapToInt(nodeIdRanges -> IntStream.rangeClosed(Math.toIntExact(nodeIdRanges.getStart()),
                        Math.toIntExact(nodeIdRanges.getEnd())));
        var upstreamNodeRoutes = upstreamNodeIds
                .mapToObj(nodeId -> routeBuilder(name(cluster) + "-" + name(ingress) + "-" + nodeId, prefixedClusterIngressHost("broker-" + nodeId)));
        return Stream.concat(Stream.of(bootstrapRoute), upstreamNodeRoutes);
    }

    private RouteBuilder routeBuilder(String routeName, String host) {
        return new RouteBuilder().withNewMetadata().withName(routeName).withNamespace(namespace(proxy))
                .withOwnerReferences(ResourcesUtil.newOwnerReferenceTo(proxy), ResourcesUtil.newOwnerReferenceTo(ingress), ResourcesUtil.newOwnerReferenceTo(cluster))
                .withLabels(Labels.standardLabels(proxy))
                .endMetadata()
                .withNewSpec().withHost(host)
                .withNewPort().withTargetPort(new IntOrString(sharedSniPort)).endPort()
                .withNewTls().withTermination("passthrough").withInsecureEdgeTerminationPolicy("None").endTls()
                .withNewTo().withKind("Service").withName(ClusterServiceDependentResource.sharedSniLoadbalancerServiceName(proxy)).endTo()
                .endSpec();
    }

    @Override
    public Optional<Tls> downstreamTls() {
        return Optional.of(tls);
    }

    @Override
    public boolean requiresSharedSniContainerPort() {
        return true;
    }

    @Override
    public Stream<Integer> requiredClientFacingPorts() {
        return Stream.of(DEFAULT_CLIENT_FACING_LOADBALANCER_PORT);
    }

    @Override
    public Annotations.ClusterIngressBootstrapServers bootstrapServersToAnnotate() {
        return new Annotations.ClusterIngressBootstrapServers(name(cluster), name(ingress), HostPort.asString(bootstrapServersHost(), ROUTE_TLS_PORT));
    }

    @NonNull
    private String bootstrapServersHost() {
        return prefixedClusterIngressHost("bootstrap");
    }

    @Override
    public Optional<SharedLoadBalancerServiceRequirements> sharedLoadBalancerServiceRequirements() {
        return Optional.of(this);
    }
}
