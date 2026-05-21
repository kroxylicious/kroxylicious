/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.systemtests.Constants;

import static io.kroxylicious.kubernetes.api.common.Protocol.TCP;

public class KroxyliciousKafkaProxyIngressTemplates {

    private KroxyliciousKafkaProxyIngressTemplates() {
    }

    /**
     * Default Kafka proxy ingress CR.
     *
     * @param ingressName the ingress name
     * @return the Kafka proxy ingress builder
     */
    public static KafkaProxyIngressBuilder defaultKafkaProxyIngressCR(String ingressName) {
        // @formatter:off
        return new KafkaProxyIngressBuilder()
                .withNewMetadata()
                    .withName(ingressName)
                .endMetadata()
                .withNewSpec()
                    .withNewClusterIP()
                        .withProtocol(TCP)
                    .endClusterIP()
                    .withNewProxyRef()
                        .withName(Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME)
                    .endProxyRef()
                .endSpec();
        // @formatter:on
    }
}
