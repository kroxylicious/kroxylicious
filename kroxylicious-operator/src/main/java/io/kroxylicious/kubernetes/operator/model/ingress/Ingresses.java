/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.ingress;

import java.util.Optional;
import java.util.stream.Stream;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP;
import io.kroxylicious.kubernetes.operator.resolver.ProxyResolutionResult;

class Ingresses {
    private Ingresses() {
    }

    public static Stream<IngressDefinition> ingressesFor(KafkaProxy primary, VirtualKafkaCluster cluster, ProxyResolutionResult resolutionResult) {
        return cluster.getSpec().getIngressRefs().stream()
                .flatMap(
                        ingressRef -> {
                            // skip unresolved ingresses, we are working with VirtualClusters that may have unresolved references
                            Optional<KafkaProxyIngress> ingress = resolutionResult.ingress(ingressRef);
                            return ingress.stream().map(kafkaProxyIngress -> toIngress(primary, cluster, kafkaProxyIngress));
                        });
    }

    private static IngressDefinition toIngress(KafkaProxy primary, VirtualKafkaCluster cluster, KafkaProxyIngress ingress) {
        ClusterIP clusterIP = ingress.getSpec().getClusterIP();
        if (clusterIP != null) {
            return new ClusterIPIngressDefinition(ingress, cluster, primary);
        }
        else {
            throw new IllegalStateException("ingress must have clusterIP specified");
        }
    }
}
