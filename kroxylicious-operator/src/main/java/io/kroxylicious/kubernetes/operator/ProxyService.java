/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.util.List;
import java.util.stream.IntStream;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.CRUDKubernetesDependentResource;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

/**
 * The Kube {@code Service} for the proxy
 */
@KubernetesDependent
public class ProxyService
        extends CRUDKubernetesDependentResource<Service, KafkaProxy> {

    public ProxyService() {
        super(Service.class);
    }

    static String serviceName(KafkaProxy primary) {
        return primary.getMetadata().getName();
    }

    static int metricsPort() {
        return 9190;
    }

    static List<Integer> brokerPorts() {
        int startPort = 9292;
        int numBrokerPorts = 4;
        return IntStream.range(startPort, startPort + numBrokerPorts).boxed().toList();
    }

    @Override
    protected Service desired(KafkaProxy primary,
                              Context<KafkaProxy> context) {
        boolean enableAdminHttp = true;
        var serviceSpecBuilder = new ServiceBuilder()
                .withMetadata(
                        new ObjectMetaBuilder()
                                .withName(serviceName(primary))
                                .withNamespace(primary.getMetadata().getNamespace())
                                .build())
                .withNewSpec()
                .withSelector(ProxyDeployment.podLabels());
        if (enableAdminHttp) {
            serviceSpecBuilder = serviceSpecBuilder
                    .addNewPort()
                    .withName("metrics")
                    .withPort(metricsPort())
                    .withTargetPort(new IntOrString(metricsPort()))
                    .withProtocol("TCP")
                    .endPort();
        }
        for (var portNum : brokerPorts()) {
            serviceSpecBuilder = serviceSpecBuilder
                    .addNewPort()
                    .withName("port-" + portNum)
                    .withPort(portNum)
                    .withTargetPort(new IntOrString(portNum))
                    .withProtocol("TCP")
                    .endPort();
        }
        return serviceSpecBuilder
                .endSpec()
                .build();
    }
}
