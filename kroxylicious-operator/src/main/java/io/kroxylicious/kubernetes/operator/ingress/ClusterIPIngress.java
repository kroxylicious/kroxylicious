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

import static io.kroxylicious.kubernetes.operator.Labels.standardLabels;

public record ClusterIPIngress(KafkaProxyIngress resource, VirtualKafkaCluster cluster, KafkaProxy primary) implements Ingress {

    // TODO replace with nodeid declaration in CRD
    private static final int NUM_BROKERS = 3;

    @Override
    public VirtualClusterGateway gatewayConfig(int proxyPortRangeStartInc, int proxyPortRangeEndInc) {
        return new VirtualClusterGateway("default",
                new PortIdentifiesNodeIdentificationStrategy(new HostPort("localhost", proxyPortRangeStartInc),
                        absoluteServiceHost(), null, List.of(new NamedRange("default", 0, NUM_BROKERS - 1))),
                null,
                Optional.empty());
    }

    @Override
    public Stream<ServiceBuilder> services(int proxyPortRangeStart, int proxyPortRangeEndInc) {
        sanityCheckPortRange(proxyPortRangeStart, proxyPortRangeEndInc);
        var serviceSpecBuilder = new ServiceBuilder()
                .withNewMetadata()
                .withName(serviceName(cluster, resource))
                .withNamespace(cluster.getMetadata().getNamespace())
                .addToLabels(standardLabels(primary))
                .addNewOwnerReferenceLike(ResourcesUtil.ownerReferenceTo(primary)).endOwnerReference()
                .endMetadata()
                .withNewSpec()
                .withSelector(ProxyDeployment.podLabels(primary));
        for (int i = proxyPortRangeStart; i <= proxyPortRangeEndInc; i++) {
            serviceSpecBuilder = serviceSpecBuilder
                    .addNewPort()
                    .withName(cluster.getMetadata().getName() + "-" + i)
                    .withPort(i)
                    .withTargetPort(new IntOrString(i))
                    .withProtocol("TCP")
                    .endPort();
        }
        return Stream.of(serviceSpecBuilder.endSpec());
    }

    @Override
    public Stream<ContainerPort> proxyContainerPorts(int startPortInc, int proxyPortRangeEndInc) {
        sanityCheckPortRange(startPortInc, proxyPortRangeEndInc);
        Stream<ContainerPort> bootstrapPort = Stream.of(new ContainerPortBuilder().withContainerPort(startPortInc)
                .withName(startPortInc + "-bootstrap").build());
        Stream<ContainerPort> ingressNodePorts = IntStream.range(0, NUM_BROKERS).mapToObj(
                nodeId -> {
                    int port = startPortInc + nodeId + 1;
                    return new ContainerPortBuilder().withContainerPort(port)
                            .withName(port + "-node").build();
                });
        return Stream.concat(bootstrapPort, ingressNodePorts);
    }

    private void sanityCheckPortRange(int startPortInc, int endPortInc) {
        if ((endPortInc - startPortInc + 1) != proxyPortsRequired()) {
            throw new IllegalArgumentException("require " + proxyPortsRequired() + " ports");
        }
    }

    public static String serviceName(VirtualKafkaCluster cluster, KafkaProxyIngress resource) {
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(resource);
        return cluster.getMetadata().getName() + "-" + resource.getMetadata().getName();
    }

    @Override
    public int proxyPortsRequired() {
        // one per broker plus the bootstrap
        return NUM_BROKERS + 1;
    }

    String absoluteServiceHost() {
        return cluster.getMetadata().getName() + "-" + resource.getMetadata().getName() + "." + cluster.getMetadata().getNamespace() + ".svc.cluster.local";
    }
}
