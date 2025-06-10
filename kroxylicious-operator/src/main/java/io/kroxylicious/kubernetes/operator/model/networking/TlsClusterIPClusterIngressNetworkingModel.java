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

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.operator.Annotations;
import io.kroxylicious.kubernetes.operator.ProxyDeploymentDependentResource;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.proxy.config.NodeIdentificationStrategy;
import io.kroxylicious.proxy.config.SniHostIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.service.HostPort;

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
        ObjectMetaBuilder metadataBuilder = baseServiceMetadataBuilder(serviceName);
        Annotations.annotateWithBootstrapServers(metadataBuilder,
                Set.of(new Annotations.ClusterIngressBootstrapServers(ResourcesUtil.name(cluster), ResourcesUtil.name(ingress), bootstrapServers())));
        var bootstrapService = createService(metadataBuilder);
        var nodeServices = getNodeServices();
        return Stream.concat(Stream.of(bootstrapService), nodeServices);
    }

    private Stream<ServiceBuilder> getNodeServices() {
        return nodeIdRanges.stream()
                .flatMapToInt(nodeIdRange -> IntStream.rangeClosed(toIntExact(nodeIdRange.getStart()), toIntExact(nodeIdRange.getEnd())))
                .mapToObj(upstreamNodeId -> {
                    ObjectMetaBuilder metadataBuilder = baseServiceMetadataBuilder(suffixedServiceName(String.valueOf(upstreamNodeId)));
                    return createService(metadataBuilder);
                });
    }

    private String suffixedServiceName(String suffix) {
        return name(cluster) + "-" + name(ingress) + "-" + suffix;
    }

    private ServiceBuilder createService(ObjectMetaBuilder metadataBuilder) {
        return new ServiceBuilder()
                .withMetadata(metadataBuilder.build())
                .withNewSpec()
                .withSelector(ProxyDeploymentDependentResource.podLabels(proxy))
                .withPorts(new ServicePortBuilder().withProtocol("TCP").withPort(CLIENT_FACING_PORT).withTargetPort(new IntOrString(sharedSniPort)).build())
                .endSpec();
    }

    private String bootstrapServiceName() {
        // we want to ensure TLS and TCP have the same service name for the service that will be used to bootstrap
        // this is because the VKC reconciler loads the Service by name.
        return TcpClusterIPClusterIngressNetworkingModel.bootstrapServiceName(cluster, name(ingress));
    }

    private ObjectMetaBuilder baseServiceMetadataBuilder(String name) {
        return new ObjectMetaBuilder()
                .withName(name)
                .withNamespace(namespace(cluster))
                .addToLabels(standardLabels(proxy))
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(proxy)).endOwnerReference()
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(cluster)).endOwnerReference()
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(ingress)).endOwnerReference();
    }

    @Override
    public Stream<ContainerPort> identifyingProxyContainerPorts() {
        return Stream.of();
    }

    @Override
    public boolean requiresSharedSniContainerPort() {
        return true;
    }

    private String bootstrapServers() {
        return new HostPort(getCrossNamespaceServiceAddress(), CLIENT_FACING_PORT).toString();
    }

    @Override
    public NodeIdentificationStrategy nodeIdentificationStrategy() {
        HostPort bootstrapAddress = new HostPort(getCrossNamespaceServiceAddress(), sharedSniPort);
        HostPort advertisedBrokerAddressPattern = new HostPort(crossNamespaceServiceAddress(suffixedServiceName("$(nodeId)"), proxy), CLIENT_FACING_PORT);
        return new SniHostIdentifiesNodeIdentificationStrategy(bootstrapAddress.toString(),
                advertisedBrokerAddressPattern.toString());
    }

    private String getCrossNamespaceServiceAddress() {
        return crossNamespaceServiceAddress(bootstrapServiceName(), proxy);
    }

    @Override
    public Optional<Tls> downstreamTls() {
        return Optional.of(tls);
    }

}
