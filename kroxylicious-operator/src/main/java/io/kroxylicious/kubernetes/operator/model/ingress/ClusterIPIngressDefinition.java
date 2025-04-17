/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.ingress;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.operator.ProxyDeploymentDependentResource;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;
import static java.lang.Math.toIntExact;

public record ClusterIPIngressDefinition(
                                         KafkaProxyIngress resource,
                                         VirtualKafkaCluster cluster,
                                         KafkaProxy primary,
                                         List<NodeIdRanges> nodeIdRanges)
        implements IngressDefinition {

    public ClusterIPIngressDefinition {
        Objects.requireNonNull(resource);
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(primary);
        Objects.requireNonNull(nodeIdRanges);
        if (nodeIdRanges.isEmpty()) {
            throw new IllegalArgumentException("nodeIdRanges cannot be empty");
        }
    }

    public record ClusterIPIngressInstance(ClusterIPIngressDefinition definition, int firstIdentifyingPort, int lastIdentifyingPort)
            implements IngressInstance {
        public ClusterIPIngressInstance {
            Objects.requireNonNull(definition);
            sanityCheckPortRange(definition, firstIdentifyingPort, lastIdentifyingPort);
        }

        @Override
        public Stream<ServiceBuilder> services() {
            var serviceSpecBuilder = new ServiceBuilder()
                    .withNewMetadata()
                    .withName(serviceName(definition.cluster, definition.resource))
                    .withNamespace(namespace(definition.cluster))
                    .addToLabels(standardLabels(definition.primary))
                    .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(definition.primary)).endOwnerReference()
                    .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(definition.cluster)).endOwnerReference()
                    .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(definition.resource)).endOwnerReference()
                    .endMetadata()
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

        private static void sanityCheckPortRange(ClusterIPIngressDefinition definition, int startPortInc, int endPortInc) {
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
    }

    @VisibleForTesting
    public static String serviceName(VirtualKafkaCluster cluster, KafkaProxyIngress resource) {
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(resource);
        return name(cluster) + "-" + name(resource);
    }

    @Override
    public ClusterIPIngressInstance createInstance(int firstIdentifyingPort, int lastIdentifyingPort) {
        return new ClusterIPIngressInstance(this, firstIdentifyingPort, lastIdentifyingPort);
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

}
