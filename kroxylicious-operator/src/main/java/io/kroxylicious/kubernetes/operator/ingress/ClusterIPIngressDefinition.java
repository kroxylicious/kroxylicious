/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.ingress;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.ProxyDeployment;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.PortIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.namespace;

record ClusterIPIngressDefinition(KafkaProxyIngress resource, VirtualKafkaCluster cluster, KafkaProxy primary) implements IngressDefinition {
    private record ClusterIPIngressInstance(@NonNull ClusterIPIngressDefinition definition, int firstIdentifyingPort, int lastIdentifyingPort)
            implements IngressInstance {
        ClusterIPIngressInstance {
            Objects.requireNonNull(definition);
            sanityCheckPortRange(definition, firstIdentifyingPort, lastIdentifyingPort);
        }

        @Override
        public VirtualClusterGateway gatewayConfig() {
            return new VirtualClusterGateway("default",
                    new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", firstIdentifyingPort),
                            qualifiedServiceHost(), null, List.of(new NamedRange("default", 0, NUM_BROKERS - 1))),
                    null,
                    Optional.empty());
        }

        @Override
        public Stream<ServiceBuilder> services() {
            var serviceSpecBuilder = new ServiceBuilder()
                    .withNewMetadata()
                    .withName(serviceName(definition.cluster, definition.resource))
                    .withNamespace(namespace(definition.cluster))
                    .addToLabels(standardLabels(definition.primary))
                    .addNewOwnerReferenceLike(ResourcesUtil.newOwnerReferenceTo(definition.primary)).endOwnerReference()
                    .endMetadata()
                    .withNewSpec()
                    .withSelector(ProxyDeployment.podLabels(definition.primary));
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
            Stream<ContainerPort> ingressNodePorts = IntStream.range(0, NUM_BROKERS).mapToObj(
                    nodeId -> {
                        int port = firstIdentifyingPort + nodeId + 1;
                        return new ContainerPortBuilder().withContainerPort(port)
                                .withName(port + "-node").build();
                    });
            return Stream.concat(bootstrapPort, ingressNodePorts);
        }

        String qualifiedServiceHost() {
            return name(definition.cluster) + "-" + name(definition.resource) + "." + namespace(definition.cluster) + ".svc.cluster.local";
        }
    }

    // TODO replace with nodeid declaration in CRD
    private static final int NUM_BROKERS = 3;

    private static String serviceName(VirtualKafkaCluster cluster, KafkaProxyIngress resource) {
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(resource);
        return name(cluster) + "-" + name(resource);
    }

    @NonNull
    @Override
    public IngressInstance createInstance(int firstIdentifyingPort, int lastIdentifyingPort) {
        return new ClusterIPIngressInstance(this, firstIdentifyingPort, lastIdentifyingPort);
    }

    @Override
    public int numIdentifyingPortsRequired() {
        // one per broker plus the bootstrap
        return NUM_BROKERS + 1;
    }

}
