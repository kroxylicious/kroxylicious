/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.kroxylicious.kubernetes.api.common.FilterRef;
import io.kroxylicious.kubernetes.api.common.FilterRefBuilder;
import io.kroxylicious.kubernetes.api.common.KafkaServiceRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;

public class KroxyliciousVirtualKafkaClusterTemplates {

    private KroxyliciousVirtualKafkaClusterTemplates() {
    }

    private static VirtualKafkaClusterBuilder baseVirtualKafkaClusterDeployment(String namespaceName, String clusterName, String proxyName, String clusterRefName,
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
        return baseVirtualKafkaClusterDeployment(namespaceName, clusterName, proxyName, clusterRefName, ingressName);
    }

    /**
     * Default virtual kafka cluster deployment.
     *
     * @param namespaceName the namespace name
     * @param clusterName the cluster name
     * @param proxyName the proxy name
     * @param clusterRefName the cluster ref name
     * @param ingressName the ingress name
     * @param filterNames the filter names
     * @return the virtual kafka cluster builder
     */
    public static VirtualKafkaClusterBuilder virtualKafkaClusterWithFilterDeployment(String namespaceName, String clusterName, String proxyName, String clusterRefName,
                                                                                  String ingressName, List<String> filterNames) {
        return baseVirtualKafkaClusterDeployment(namespaceName, clusterName, proxyName, clusterRefName, ingressName)
                .editSpec()
                .addAllToFilterRefs(getFilterRefs(filterNames))
//                .addToFilterRefs(new FilterRefBuilder()
//                        .withName(filterName)
//                        .build())
                .endSpec();
    }

    private static Collection<FilterRef> getFilterRefs(List<String> filterNames) {
        List<FilterRef> filterRefs = new ArrayList<>();
        filterNames.forEach(filter -> filterRefs.add(new FilterRefBuilder().withName(filter).build()));
        return filterRefs;
    }
}
