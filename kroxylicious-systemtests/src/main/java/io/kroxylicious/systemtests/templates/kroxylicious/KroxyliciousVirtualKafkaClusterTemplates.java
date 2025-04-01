/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import io.kroxylicious.kubernetes.api.common.KafkaServiceRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;

public class KroxyliciousVirtualKafkaClusterTemplates {

    private KroxyliciousVirtualKafkaClusterTemplates() {
    }

    /**
     * Default virtual kafka cluster deployment.
     *
     * @param namespaceName the namespace name
     * @param clusterName the cluster name
     * @param proxyName the proxy name
     * @param clusterRefName the cluster ref name
     * @param ingressName the ingress name
     * @return the virtual kafka cluster builder
     */
    public static VirtualKafkaClusterBuilder defaultVirtualKafkaClusterDeployment(String namespaceName, String clusterName, String proxyName, String clusterRefName,
                                                                                  String ingressName) {
        // @formatter:off
        return new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespaceName)
                .endMetadata()
                .withNewSpec()
                    .withTargetKafkaServiceRef(new KafkaServiceRefBuilder()
                            .withName(clusterRefName)
                            .build())
                    .withNewProxyRef()
                        .withName(proxyName)
                    .endProxyRef()
                    .addNewIngressRef()
                        .withName(ingressName)
                    .endIngressRef()
                .endSpec();
        // @formatter:on
    }
}
