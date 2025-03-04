/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.ingress;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;

import static io.kroxylicious.kubernetes.operator.ProxyDeployment.PROXY_PORT_START;

public record ProxyIngressLayout(List<VirtualClusterLayout> clusterLayouts) {

    public Optional<VirtualClusterLayout> clusterLayout(VirtualKafkaCluster cluster) {
        return clusterLayouts.stream()
                .filter(c -> c.cluster.getMetadata().getName().equals(cluster.getMetadata().getName()))
                .findFirst();
    }

    public record VirtualClusterLayout(VirtualKafkaCluster cluster, List<IngressLayout> ingressLayouts) {
        public List<VirtualClusterGateway> gateways() {
            return ingressLayouts.stream().map(ingressLayout -> {
                int proxyPortRangeEnd = ingressLayout.endPortExc() - 1;
                return ingressLayout.ingressModel.gatewayConfig(ingressLayout.startPortInc(), proxyPortRangeEnd);
            }).toList();
        }

        public Stream<Service> services() {
            return ingressLayouts.stream().flatMap(it -> it.ingressModel().services(it.startPortInc(), it.endPortExc() - 1))
                    .map(ServiceBuilder::build);
        }
    }

    public record IngressLayout(Ingress ingressModel, int startPortInc, int endPortExc) {

        public Stream<ContainerPort> proxyContainerPorts() {
            return ingressModel.proxyContainerPorts(startPortInc, endPortExc - 1);
        }
    }

    public static ProxyIngressLayout layout(KafkaProxy primary, List<VirtualKafkaCluster> clusters, Set<KafkaProxyIngress> ingressResources) {
        AtomicInteger ports = new AtomicInteger(PROXY_PORT_START);
        // include broken clusters in the layout, so that if they are healed the ports will stay the same
        Stream<VirtualKafkaCluster> virtualKafkaClusterStream = clusters.stream();
        List<VirtualClusterLayout> list = virtualKafkaClusterStream.map(it -> new VirtualClusterLayout(it, ingressLayout(primary, it, ports,
                ingressResources))).toList();
        return new ProxyIngressLayout(list);
    }

    private static List<IngressLayout> ingressLayout(KafkaProxy primary, VirtualKafkaCluster it, AtomicInteger ports, Set<KafkaProxyIngress> ingressResources) {
        Stream<Ingress> ingressStream = Ingresses.ingressesFor(primary, it, ingressResources).toList().stream();
        return ingressStream.map(resource -> {
            int toAllocate = resource.proxyPortsRequired();
            return new IngressLayout(resource, ports.get(), ports.addAndGet(toAllocate));
        }).toList();
    }
}
