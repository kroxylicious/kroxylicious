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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.proxy.config.VirtualClusterGateway;

import static io.kroxylicious.kubernetes.operator.ProxyDeployment.PROXY_PORT_START;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;

public record ProxyIngressLayout(List<VirtualClusterLayout> clusterLayouts) {

    public Optional<VirtualClusterLayout> clusterLayout(VirtualKafkaCluster cluster) {
        return clusterLayouts.stream()
                .filter(c -> name(c.cluster).equals(name(cluster)))
                .findFirst();
    }

    public record VirtualClusterLayout(VirtualKafkaCluster cluster, List<IngressLayout> ingressLayouts) {
        public List<VirtualClusterGateway> gateways() {
            return ingressLayouts.stream().map(ingressLayout -> ingressLayout.ingressInstance.gatewayConfig()).toList();
        }

        public Stream<Service> services() {
            return ingressLayouts.stream().flatMap(it -> it.ingressInstance().services()).map(ServiceBuilder::build);
        }

        public Set<IngressConflictException> ingressExceptions() {
            return ingressLayouts.stream().flatMap(it -> Stream.ofNullable(it.exception)).collect(Collectors.toSet());
        }
    }

    public record IngressLayout(IngressInstance ingressInstance, IngressConflictException exception) {

        public Stream<ContainerPort> proxyContainerPorts() {
            return ingressInstance.proxyContainerPorts();
        }

    }

    public static ProxyIngressLayout layout(KafkaProxy primary, List<VirtualKafkaCluster> clusters, Set<KafkaProxyIngress> ingressResources) {
        AtomicInteger exclusivePorts = new AtomicInteger(PROXY_PORT_START);
        // include broken clusters in the layout, so that if they are healed the ports will stay the same
        Stream<VirtualKafkaCluster> virtualKafkaClusterStream = clusters.stream();
        List<VirtualClusterLayout> list = virtualKafkaClusterStream.map(it -> new VirtualClusterLayout(it, ingressLayout(primary, it, exclusivePorts,
                ingressResources))).toList();
        return new ProxyIngressLayout(list);
    }

    private static List<IngressLayout> ingressLayout(KafkaProxy primary, VirtualKafkaCluster it, AtomicInteger ports, Set<KafkaProxyIngress> ingressResources) {
        Stream<IngressDefinition> ingressStream = Ingresses.ingressesFor(primary, it, ingressResources).toList().stream();
        return ingressStream.map(resource -> {
            int toAllocate = resource.identityfulPortsRequired();
            IngressConflictException exception = null;
            if (ports.get() != PROXY_PORT_START) {
                exception = new IngressConflictException(name(resource.resource()),
                        "Currently we do not support a virtual cluster with multiple ingresses that need unique ports to identify which node the "
                                + "client is connecting to. We currently do not have a sufficient strategy for port allocation for this case. See https://github.com/kroxylicious/kroxylicious/issues/1902");
            }
            int firstIdentityfulPort = ports.get();
            int lastIdentityfulPort = ports.addAndGet(toAllocate) - 1;
            return new IngressLayout(resource.createInstance(firstIdentityfulPort, lastIdentityfulPort), exception);
        }).toList();
    }
}
