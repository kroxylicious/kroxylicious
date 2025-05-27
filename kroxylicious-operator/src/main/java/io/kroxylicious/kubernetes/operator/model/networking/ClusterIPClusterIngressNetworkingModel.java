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
import io.kroxylicious.kubernetes.operator.ProxyDeploymentDependentResource;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.NodeIdentificationStrategy;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static java.lang.Math.toIntExact;

public record ClusterIPClusterIngressNetworkingModel(KafkaProxy proxy,
                                                     VirtualKafkaCluster cluster,
                                                     KafkaProxyIngress ingress,
                                                     List<NodeIdRanges> nodeIdRanges,
                                                     @Nullable Tls tls,
                                                     int firstIdentifyingPort,
                                                     int lastIdentifyingPort)
        implements ClusterIngressNetworkingModel {

    public ClusterIPClusterIngressNetworkingModel {
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
        return name(cluster) + "-" + ingressName;
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
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(proxy)).endOwnerReference()
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(cluster)).endOwnerReference()
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(ingress)).endOwnerReference()
                .build();
    }

    @Override
    public Stream<ContainerPort> proxyContainerPorts() {
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
                crossNamespaceServiceAddress(),
                null,
                portRanges);
    }

    /**
     * @return an address that any pod in the same k8s cluster can use to address the service, regardless of which namespace the pod is in
     */
    private String crossNamespaceServiceAddress() {
        return bootstrapServiceName() + "." + namespace(cluster) + ".svc.cluster.local";
    }

    @Override
    public Optional<Tls> downstreamTls() {
        return Optional.ofNullable(tls);
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
