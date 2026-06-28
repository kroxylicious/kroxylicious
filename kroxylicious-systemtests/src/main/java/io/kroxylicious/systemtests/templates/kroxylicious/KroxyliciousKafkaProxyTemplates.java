/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.systemtests.Constants;

public class KroxyliciousKafkaProxyTemplates {

    private KroxyliciousKafkaProxyTemplates() {
    }

    /**
     * Default Kafka proxy CR.
     *
     * @param replicas the number proxy pods to deploy
     * @return the Kafka proxy builder
     */
    public static KafkaProxyBuilder defaultKafkaProxyCR(int replicas) {
        // @formatter:off
        return new KafkaProxyBuilder()
                .withNewMetadata()
                    .withName(Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME)
                .endMetadata()
                .withNewSpec()
                    .withReplicas(replicas)
                .endSpec();
        // @formatter:on
    }
}
