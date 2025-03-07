/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.ingress;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP;
import io.kroxylicious.kubernetes.operator.Resources;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.Resources.name;

public class Ingresses {

    public static Stream<Ingress> ingressesFor(KafkaProxy primary, VirtualKafkaCluster cluster, Set<KafkaProxyIngress> ingressResources) {
        Map<String, KafkaProxyIngress> namedIngresses = Resources.indexByName(ingressResources.stream());
        return cluster.getSpec().getIngressRefs().stream().map(io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressRefs::getName).map(
                ingressName -> {
                    if (!namedIngresses.containsKey(ingressName)) {
                        throw new IllegalStateException(
                                "VirtualKafkaCluster " + name(cluster) + " references an Ingress " + ingressName
                                        + "that isn't associated with the proxy");
                    }
                    else {
                        return toIngress(primary, cluster, namedIngresses.get(ingressName));
                    }
                });
    }

    private static @NonNull Ingress toIngress(KafkaProxy primary, VirtualKafkaCluster cluster, KafkaProxyIngress ingress) {
        ClusterIP clusterIP = ingress.getSpec().getClusterIP();
        if (clusterIP != null) {
            return new ClusterIPIngress(ingress, cluster, primary);
        }
        else {
            throw new IllegalStateException("ingress must have clusterIP specified");
        }
    }
}
