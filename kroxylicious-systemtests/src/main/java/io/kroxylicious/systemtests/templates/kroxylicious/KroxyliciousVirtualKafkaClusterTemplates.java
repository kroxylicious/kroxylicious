/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import io.kroxylicious.kubernetes.api.common.KafkaServiceRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;

public class KroxyliciousVirtualKafkaClusterTemplates {

    private KroxyliciousVirtualKafkaClusterTemplates() {
    }

    /**
     * Default virtual kafka cluster deployment.
     *
     * @param namespaceName the namespace name
     * @param clusterName the cluster name
     * @param proxy the proxy
     * @param clusterRef the cluster ref
     * @param ingress the ingress
     * @return the virtual kafka cluster builder
     */
    public static VirtualKafkaClusterBuilder defaultVirtualKafkaClusterDeployment(String namespaceName, String clusterName, KafkaProxy proxy, KafkaService clusterRef,
                                                                                  KafkaProxyIngress ingress) {
        // @formatter:off
        return new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                    .withNamespace(namespaceName)
                .endMetadata()
                .withNewSpec()
                    .withTargetKafkaServiceRef(new KafkaServiceRefBuilder()
                            .withName(name(clusterRef))
                            .build())
                    .withNewProxyRef()
                        .withName(name(proxy))
                    .endProxyRef()
                    .addNewIngressRef()
                        .withName(name(ingress))
                    .endIngressRef()
                .endSpec();
        // @formatter:on
    }
}
