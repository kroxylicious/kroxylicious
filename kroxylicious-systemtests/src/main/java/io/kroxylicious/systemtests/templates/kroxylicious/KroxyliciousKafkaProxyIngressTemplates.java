/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;

import static io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP.Protocol.TCP;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;

public class KroxyliciousKafkaProxyIngressTemplates {

    private KroxyliciousKafkaProxyIngressTemplates() {
    }

    /**
     * Default kafka proxy ingress deployment.
     *
     * @param namespaceName the namespace name
     * @param ingressName the ingress name
     * @param proxy the proxy
     * @return the kafka proxy ingress builder
     */
    public static KafkaProxyIngressBuilder defaultKafkaProxyIngressDeployment(String namespaceName, String ingressName, KafkaProxy proxy) {
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
                        .withName(name(proxy))
                    .endProxyRef()
                .endSpec();
        // @formatter:on
    }
}
