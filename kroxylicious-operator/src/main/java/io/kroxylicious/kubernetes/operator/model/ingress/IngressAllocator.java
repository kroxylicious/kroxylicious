/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.ingress;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult;

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

    /**
     * Allocates a ProxyIngressModel. The aim is to deterministically produce a model of the ports that will
     * be used by the proxy container. We want this to be as stable as possible, so we will allocate ports
     * to potentially unacceptable virtual clusters if we can.
     *
     * @param primary primary being reconciled
     * @param context context
     * @param resolutionResult
     * @return non-null ProxyIngressModel
     */
    public static ProxyIngressModel allocateProxyIngressModel(KafkaProxy primary,
                                                              Context<KafkaProxy> context, ResolutionResult resolutionResult) {
        AtomicInteger exclusivePorts = new AtomicInteger(PROXY_PORT_START);
        // include broken clusters in the model, so that if they are healed the ports will stay the same
        Stream<VirtualKafkaCluster> virtualKafkaClusterStream = resolutionResult.allClustersInNameOrder().stream();
        List<ProxyIngressModel.VirtualClusterIngressModel> list = virtualKafkaClusterStream
                .map(it -> new ProxyIngressModel.VirtualClusterIngressModel(it, allocateIngressModel(primary, it, exclusivePorts,
                        resolutionResult)))
                .toList();
        ProxyIngressModel model = new ProxyIngressModel(list);
        return model;
    }

    private static List<ProxyIngressModel.IngressModel> allocateIngressModel(KafkaProxy primary, VirtualKafkaCluster it, AtomicInteger ports,
                                                                             ResolutionResult resolutionResult) {
        Stream<IngressDefinition> ingressStream = Ingresses.ingressesFor(primary, it, resolutionResult);
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
