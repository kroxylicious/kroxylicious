/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.ingress;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRangesBuilder;
import io.kroxylicious.kubernetes.operator.resolver.ProxyResolutionResult;

class Ingresses {

    public static final List<NodeIdRanges> DEFAULT_NODE_ID_RANGES = List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build());

    private Ingresses() {
    }

    public static Stream<IngressDefinition> ingressesFor(KafkaProxy primary, VirtualKafkaCluster cluster, ProxyResolutionResult resolutionResult) {
        return cluster.getSpec().getIngressRefs().stream()
                .flatMap(
                        ingressRef -> {
                            // skip unresolved ingresses, we are working with VirtualClusters that may have dangling references
                            Optional<KafkaProxyIngress> ingress = resolutionResult.ingress(ingressRef);
                            List<NodeIdRanges> nodeIdRanges = resolutionResult.kafkaServiceRef(cluster).map(s -> s.getSpec().getNodeIdRanges())
                                    .orElse(DEFAULT_NODE_ID_RANGES);
                            return ingress.stream().map(kafkaProxyIngress -> toIngress(primary, cluster, kafkaProxyIngress, nodeIdRanges));
                        });
    }

    private static IngressDefinition toIngress(KafkaProxy primary, VirtualKafkaCluster cluster, KafkaProxyIngress ingress, List<NodeIdRanges> nodeIdRanges) {
        ClusterIP clusterIP = ingress.getSpec().getClusterIP();
        if (clusterIP != null) {
            return new ClusterIPIngressDefinition(ingress, cluster, primary, nodeIdRanges);
        }
        else {
            throw new IllegalStateException("ingress must have clusterIP specified");
        }
    }
}
