/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;

import static io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP.Protocol.TCP;

public class KroxyliciousKafkaProxyIngressTemplates {

    private KroxyliciousKafkaProxyIngressTemplates() {
    }

    /**
     * Default kafka proxy ingress deployment.
     *
     * @param namespaceName the namespace name
     * @param ingressName the ingress name
     * @param proxyName the name of the proxy to reference
     * @return the kafka proxy ingress builder
     */
    public static KafkaProxyIngressBuilder defaultKafkaProxyIngressDeployment(String namespaceName, String ingressName, String proxyName) {
        // @formatter:off
        return new KafkaProxyIngressBuilder()
                .withNewMetadata()
                    .withName(ingressName)
                    .withNamespace(namespaceName)
                .endMetadata()
                .withNewSpec()
                    .withNewClusterIP()
                        .withProtocol(TCP)
                    .endClusterIP()
                    .withNewProxyRef()
                        .withName(proxyName)
                    .endProxyRef()
                .endSpec();
        // @formatter:on
    }
}
