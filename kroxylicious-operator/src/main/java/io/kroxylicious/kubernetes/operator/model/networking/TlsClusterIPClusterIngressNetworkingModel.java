/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.operator.ProxyDeploymentDependentResource;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.proxy.config.NodeIdentificationStrategy;
import io.kroxylicious.proxy.config.SniHostIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.crossNamespaceServiceAddress;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static java.lang.Math.toIntExact;

public record TlsClusterIPClusterIngressNetworkingModel(KafkaProxy proxy,
                                                        VirtualKafkaCluster cluster,
                                                        KafkaProxyIngress ingress,
                                                        List<NodeIdRanges> nodeIdRanges,
                                                        Tls tls,
                                                        int sharedSniPort)
        implements ClusterIngressNetworkingModel {

    public static final int CLIENT_FACING_PORT = 9292;

    public TlsClusterIPClusterIngressNetworkingModel {
        Objects.requireNonNull(proxy);
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(ingress);
        Objects.requireNonNull(nodeIdRanges);
        Objects.requireNonNull(tls);
        if (nodeIdRanges.isEmpty()) {
            throw new IllegalArgumentException("nodeIdRanges cannot be empty");
        }
    }

    @Override
    public Stream<ServiceBuilder> services() {
        String serviceName = bootstrapServiceName();
        var bootstrapService = createService(serviceName);
        var nodeServices = nodeIdRanges.stream()
                .flatMapToInt(nodeIdRange -> IntStream.rangeClosed(toIntExact(nodeIdRange.getStart()), toIntExact(nodeIdRange.getEnd())))
                .mapToObj(upstreamNodeId -> createService(bootstrapServiceName() + "-" + upstreamNodeId));
        return Stream.concat(Stream.of(bootstrapService), nodeServices);
    }

    private ServiceBuilder createService(String serviceName) {
        return new ServiceBuilder()
                .withMetadata(serviceMetadata(serviceName))
                .withNewSpec()
                .withSelector(ProxyDeploymentDependentResource.podLabels(proxy))
                .withPorts(new ServicePortBuilder().withProtocol("TCP").withPort(CLIENT_FACING_PORT).withTargetPort(new IntOrString(sharedSniPort)).build())
                .endSpec();
    }

    @NonNull
    private String bootstrapServiceName() {
        // we want to ensure TLS and TCP have the same service name for the service that will be used to bootstrap
        // this is because the VKC reconciler loads the Service by name.
        return TcpClusterIPClusterIngressNetworkingModel.bootstrapServiceName(cluster, name(ingress));
    }

    ObjectMeta serviceMetadata(String name) {
        return new ObjectMetaBuilder()
                .withName(name)
                .withNamespace(namespace(cluster))
                .addToLabels(standardLabels(proxy))
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(proxy)).endOwnerReference()
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(cluster)).endOwnerReference()
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(ingress)).endOwnerReference()
                .build();
    }

    @Override
    public Stream<ContainerPort> identifyingProxyContainerPorts() {
        return Stream.of();
    }

    @Override
    public boolean requiresSharedSniContainerPort() {
        return true;
    }

    @Override
    public NodeIdentificationStrategy nodeIdentificationStrategy() {
        HostPort bootstrapAddress = new HostPort(crossNamespaceServiceAddress(bootstrapServiceName(), proxy), sharedSniPort);
        HostPort advertisedBrokerAddressPattern = new HostPort(crossNamespaceServiceAddress(bootstrapServiceName() + "-$(nodeId)", proxy), CLIENT_FACING_PORT);
        return new SniHostIdentifiesNodeIdentificationStrategy(bootstrapAddress.toString(),
                advertisedBrokerAddressPattern.toString());
    }

    @Override
    public Optional<Tls> downstreamTls() {
        return Optional.of(tls);
    }

}
