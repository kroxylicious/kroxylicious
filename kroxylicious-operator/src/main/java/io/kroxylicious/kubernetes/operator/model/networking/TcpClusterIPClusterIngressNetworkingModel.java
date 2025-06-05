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
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.operator.BootstrapServersAnnotation;
import io.kroxylicious.kubernetes.operator.ProxyDeploymentDependentResource;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.NodeIdentificationStrategy;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static java.lang.Math.toIntExact;

public record TcpClusterIPClusterIngressNetworkingModel(KafkaProxy proxy,
                                                        VirtualKafkaCluster cluster,
                                                        KafkaProxyIngress ingress,
                                                        List<NodeIdRanges> nodeIdRanges,
                                                        int firstIdentifyingPort,
                                                        int lastIdentifyingPort)
        implements ClusterIngressNetworkingModel {

    public TcpClusterIPClusterIngressNetworkingModel {
        Objects.requireNonNull(proxy);
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(ingress);
        Objects.requireNonNull(nodeIdRanges);
        if (nodeIdRanges.isEmpty()) {
            throw new IllegalArgumentException("nodeIdRanges cannot be empty");
        }
        int numIdentifyingPorts = lastIdentifyingPort - firstIdentifyingPort + 1;
        int numIdentifyingPortsRequired = numIdentifyingPortsRequired(nodeIdRanges);
        if (numIdentifyingPortsRequired != numIdentifyingPorts) {
            throw new IllegalArgumentException("number of identifying ports allocated %d does not match the number required %d"
                    .formatted(numIdentifyingPorts, numIdentifyingPortsRequired));
        }
    }

    public static String bootstrapServiceName(VirtualKafkaCluster cluster, String ingressName) {
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(ingressName);
        return name(cluster) + "-" + ingressName + "-bootstrap";
    }

    @Override
    public Stream<ServiceBuilder> services() {
        String serviceName = bootstrapServiceName();
        var serviceSpecBuilder = new ServiceBuilder()
                .withMetadata(serviceMetadata(serviceName))
                .withNewSpec()
                .withSelector(ProxyDeploymentDependentResource.podLabels(proxy));
        for (int i = firstIdentifyingPort; i <= lastIdentifyingPort; i++) {
            serviceSpecBuilder = serviceSpecBuilder
                    .addNewPort()
                    .withName(name(cluster) + "-" + i)
                    .withPort(i)
                    .withTargetPort(new IntOrString(i))
                    .withProtocol("TCP")
                    .endPort();
        }
        return Stream.of(serviceSpecBuilder.endSpec());
    }

    @NonNull
    private String bootstrapServiceName() {
        return bootstrapServiceName(cluster, name(ingress));
    }

    ObjectMeta serviceMetadata(String name) {
        return new ObjectMetaBuilder()
                .withName(name)
                .withNamespace(namespace(cluster))
                .addToLabels(standardLabels(proxy))
                .addToAnnotations(BootstrapServersAnnotation.BOOTSTRAP_SERVERS_ANNOTATION_KEY, bootstrapServersAnnotation())
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(proxy)).endOwnerReference()
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(cluster)).endOwnerReference()
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(ingress)).endOwnerReference()
                .build();
    }

    @NonNull
    private String bootstrapServersAnnotation() {
        return BootstrapServersAnnotation.toAnnotation(
                Set.of(new BootstrapServersAnnotation.BootstrapServer(ResourcesUtil.name(cluster), ResourcesUtil.name(ingress), bootstrapServers())));
    }

    @Override
    public Stream<ContainerPort> identifyingProxyContainerPorts() {
        Stream<ContainerPort> bootstrapPort = Stream.of(new ContainerPortBuilder().withContainerPort(firstIdentifyingPort)
                .withName(firstIdentifyingPort + "-bootstrap").build());
        int nodeCount = lastIdentifyingPort - firstIdentifyingPort;
        Stream<ContainerPort> ingressNodePorts = IntStream.range(0, nodeCount).mapToObj(
                nodeIdx -> {
                    int port = firstIdentifyingPort + nodeIdx + 1;
                    return new ContainerPortBuilder().withContainerPort(port)
                            .withName(port + "-node").build();
                });
        return Stream.concat(bootstrapPort, ingressNodePorts);
    }

    @Override
    public NodeIdentificationStrategy nodeIdentificationStrategy() {
        List<NamedRange> portRanges = IntStream.range(0, nodeIdRanges.size()).mapToObj(i -> {
            NodeIdRanges range = nodeIdRanges.get(i);
            String name = Optional.ofNullable(range.getName()).orElse("range-" + i);
            return new NamedRange(name, toIntExact(range.getStart()), toIntExact(range.getEnd()));
        }).toList();
        return new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", firstIdentifyingPort()),
                crossNamespaceBootstrapServiceAddress(),
                null,
                portRanges);
    }

    @NonNull
    private String crossNamespaceBootstrapServiceAddress() {
        return ResourcesUtil.crossNamespaceServiceAddress(bootstrapServiceName(), cluster);
    }

    @Override
    public Optional<Tls> downstreamTls() {
        return Optional.empty();
    }

    @Override
    public String bootstrapServers() {
        return new HostPort(crossNamespaceBootstrapServiceAddress(), firstIdentifyingPort()).toString();
    }

    public static int numIdentifyingPortsRequired(List<NodeIdRanges> nodeIdRanges) {
        // one per broker plus the bootstrap
        return nodeCount(nodeIdRanges) + 1;
    }

    // note: we use CRD validation to enforce end >= start at the apiserver level
    public static int nodeCount(List<NodeIdRanges> nodeIdRanges) {
        Objects.requireNonNull(nodeIdRanges);
        if (nodeIdRanges.isEmpty()) {
            throw new IllegalArgumentException("nodeIdRanges cannot be empty");
        }
        return nodeIdRanges.stream().mapToInt(range -> toIntExact((range.getEnd() - range.getStart()) + 1)).sum();
    }

}
