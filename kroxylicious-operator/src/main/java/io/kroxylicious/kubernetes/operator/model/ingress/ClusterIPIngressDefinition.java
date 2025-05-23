/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.ingress;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
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

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static java.lang.Math.toIntExact;

public record ClusterIPIngressDefinition(
                                         KafkaProxyIngress ingress,
                                         VirtualKafkaCluster cluster,
                                         KafkaProxy primary,
                                         List<NodeIdRanges> nodeIdRanges,
                                         @Nullable Tls tls)
        implements IngressDefinition {

    public ClusterIPIngressDefinition {
        Objects.requireNonNull(ingress);
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(primary);
        Objects.requireNonNull(nodeIdRanges);
        if (nodeIdRanges.isEmpty()) {
            throw new IllegalArgumentException("nodeIdRanges cannot be empty");
        }
    }

    private record PortIdentifiesNodeClusterIPIngressModel(ClusterIPIngressDefinition definition, int firstIdentifyingPort, int lastIdentifyingPort)
            implements IngressModel {
        public PortIdentifiesNodeClusterIPIngressModel {
            Objects.requireNonNull(definition);
            sanityCheckPortRange(definition, firstIdentifyingPort, lastIdentifyingPort);
        }

        @Override
        public KafkaProxyIngress ingress() {
            return definition.ingress();
        }

        @Override
        public Stream<ServiceBuilder> services() {
            String serviceName = bootstrapServiceName(definition.cluster, name(definition.ingress));
            var serviceSpecBuilder = new ServiceBuilder()
                    .withMetadata(definition.serviceMetadata(serviceName))
                    .withNewSpec()
                    .withSelector(ProxyDeploymentDependentResource.podLabels(definition.primary));
            for (int i = firstIdentifyingPort; i <= lastIdentifyingPort; i++) {
                serviceSpecBuilder = serviceSpecBuilder
                        .addNewPort()
                        .withName(name(definition.cluster) + "-" + i)
                        .withPort(i)
                        .withTargetPort(new IntOrString(i))
                        .withProtocol("TCP")
                        .endPort();
            }
            return Stream.of(serviceSpecBuilder.endSpec());
        }

        private static void sanityCheckPortRange(ClusterIPIngressDefinition definition, Integer startPortInc, Integer endPortInc) {
            int requiredPorts = definition.numIdentifyingPortsRequired();
            if ((endPortInc - startPortInc + 1) != requiredPorts) {
                throw new IllegalArgumentException("require " + requiredPorts + " ports");
            }
        }

        @Override
        public Stream<ContainerPort> proxyContainerPorts() {
            Stream<ContainerPort> bootstrapPort = Stream.of(new ContainerPortBuilder().withContainerPort(firstIdentifyingPort)
                    .withName(firstIdentifyingPort + "-bootstrap").build());
            Stream<ContainerPort> ingressNodePorts = IntStream.range(0, definition().nodeCount()).mapToObj(
                    nodeIdx -> {
                        int port = firstIdentifyingPort + nodeIdx + 1;
                        return new ContainerPortBuilder().withContainerPort(port)
                                .withName(port + "-node").build();
                    });
            return Stream.concat(bootstrapPort, ingressNodePorts);
        }

        @Override
        public NodeIdentificationStrategy nodeIdentificationStrategy() {
            List<NamedRange> portRanges = IntStream.range(0, definition().nodeIdRanges().size()).mapToObj(i -> {
                NodeIdRanges range = definition().nodeIdRanges().get(i);
                String name = Optional.ofNullable(range.getName()).orElse("range-" + i);
                return new NamedRange(name, toIntExact(range.getStart()), toIntExact(range.getEnd()));
            }).toList();
            return new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", firstIdentifyingPort()),
                    definition.qualifiedServiceHost(), null,
                    portRanges);
        }

        @Override
        public Optional<Tls> downstreamTls() {
            return Optional.ofNullable(definition.tls());
        }
    }

    private ObjectMeta serviceMetadata(String name) {
        return new ObjectMetaBuilder()
                .withName(name)
                .withNamespace(namespace(cluster))
                .addToLabels(standardLabels(primary))
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(primary)).endOwnerReference()
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(cluster)).endOwnerReference()
                .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(ingress)).endOwnerReference()
                .build();
    }

    public static String bootstrapServiceName(VirtualKafkaCluster cluster, String ingressName) {
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(ingressName);
        return name(cluster) + "-" + ingressName;
    }

    private String qualifiedServiceHost() {
        return qualified(cluster(), bootstrapServiceName(cluster(), name(ingress())));
    }

    @Override
    public IngressModel createIngressModel(@Nullable Integer firstIdentifyingPort, @Nullable Integer lastIdentifyingPort, @Nullable Integer sharedSniPort) {
        Objects.requireNonNull(firstIdentifyingPort);
        Objects.requireNonNull(lastIdentifyingPort);
        return new PortIdentifiesNodeClusterIPIngressModel(this, firstIdentifyingPort, lastIdentifyingPort);
    }

    @Override
    public int numIdentifyingPortsRequired() {
        // one per broker plus the bootstrap
        return nodeCount() + 1;
    }

    // note: we use CRD validation to enforce end >= start at the apiserver level
    private int nodeCount() {
        return nodeIdRanges.stream().mapToInt(range -> toIntExact((range.getEnd() - range.getStart()) + 1)).sum();
    }

    public static String qualified(HasMetadata resource, String serviceName) {
        return serviceName + "." + namespace(resource) + ".svc.cluster.local";
    }

}
