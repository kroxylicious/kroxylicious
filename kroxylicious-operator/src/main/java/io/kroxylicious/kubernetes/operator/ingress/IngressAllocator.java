/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.ingress;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.ClusterCondition;
import io.kroxylicious.kubernetes.operator.SharedKafkaProxyContext;

import static io.kroxylicious.kubernetes.operator.ProxyDeployment.PROXY_PORT_START;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;

/**
 * IngressAllocator is responsible for:
 * <ul>
 *     <li>building a ProxyIngressModel with logical ingress model classes created per VirtualCluster</li>
 *     <li>allocating identifying container ports to ingresses</li>
 *     <li>deciding which ingress instances are in conflict with each other and selecting which to accept</li>
 * </ul>
 */
public class IngressAllocator {
    private IngressAllocator() {
    }

    public static ProxyIngressModel allocateProxyIngressModel(KafkaProxy primary, List<VirtualKafkaCluster> clusters, Set<KafkaProxyIngress> ingressResources,
                                                              Context<KafkaProxy> context) {
        AtomicInteger exclusivePorts = new AtomicInteger(PROXY_PORT_START);
        // include broken clusters in the model, so that if they are healed the ports will stay the same
        Stream<VirtualKafkaCluster> virtualKafkaClusterStream = clusters.stream();
        List<ProxyIngressModel.VirtualClusterIngressModel> list = virtualKafkaClusterStream
                .map(it -> new ProxyIngressModel.VirtualClusterIngressModel(it, allocateIngressModel(primary, it, exclusivePorts,
                        ingressResources)))
                .toList();
        ProxyIngressModel model = new ProxyIngressModel(list);
        reportIngressProblems(context, model);
        return model;
    }

    private static void reportIngressProblems(Context<KafkaProxy> context, ProxyIngressModel model) {
        for (ProxyIngressModel.VirtualClusterIngressModel virtualClusterIngressModel : model.clusters()) {
            Set<IngressConflictException> exceptions = virtualClusterIngressModel.ingressExceptions();
            if (!exceptions.isEmpty()) {
                VirtualKafkaCluster cluster = virtualClusterIngressModel.cluster();
                SharedKafkaProxyContext.addClusterCondition(context, cluster, ClusterCondition.ingressConflict(name(cluster), exceptions));
            }
        }
    }

    private static List<ProxyIngressModel.IngressModel> allocateIngressModel(KafkaProxy primary, VirtualKafkaCluster it, AtomicInteger ports,
                                                                             Set<KafkaProxyIngress> ingressResources) {
        Stream<IngressDefinition> ingressStream = Ingresses.ingressesFor(primary, it, ingressResources);
        return ingressStream.map(resource -> {
            int toAllocate = resource.numIdentifyingPortsRequired();
            IngressConflictException exception = null;
            if (ports.get() != PROXY_PORT_START) {
                exception = new IngressConflictException(name(resource.resource()),
                        "Currently we do not support a virtual cluster with multiple ingresses that need unique ports to identify which node the "
                                + "client is connecting to. We currently do not have a sufficient strategy for port allocation for this case. See https://github.com/kroxylicious/kroxylicious/issues/1902");
            }
            int firstIdentifyingPort = ports.get();
            int lastIdentifyingPort = ports.addAndGet(toAllocate) - 1;
            return new ProxyIngressModel.IngressModel(resource.createInstance(firstIdentifyingPort, lastIdentifyingPort), exception);
        }).toList();
    }
}
