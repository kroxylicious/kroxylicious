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
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.Tls;

public class KroxyliciousVirtualKafkaClusterTemplates {

    private KroxyliciousVirtualKafkaClusterTemplates() {
    }

    private static VirtualKafkaClusterBuilder baseVirtualKafkaClusterCR(String namespaceName, String clusterName, String proxyName, String clusterRefName,
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
                .addNewIngress()
                    .withNewIngressRef()
                        .withName(ingressName)
                    .endIngressRef()
                .endIngress()
                .endSpec();
        // @formatter:on
    }

    /**
     * Default virtual kafka cluster CR.
     *
     * @param namespaceName the namespace name
     * @param clusterName the cluster name
     * @param proxyName the proxy name
     * @param clusterRefName the cluster ref name
     * @param ingressName the ingress name
     * @return the virtual kafka cluster builder
     */
    public static VirtualKafkaClusterBuilder defaultVirtualKafkaClusterCR(String namespaceName, String clusterName, String proxyName, String clusterRefName,
                                                                          String ingressName) {
        return baseVirtualKafkaClusterCR(namespaceName, clusterName, proxyName, clusterRefName, ingressName);
    }

    /**
     * Default virtual kafka cluster CR.
     *
     * @param namespaceName the namespace name
     * @param clusterName the cluster name
     * @param proxyName the proxy name
     * @param clusterRefName the cluster ref name
     * @param ingressName the ingress name
     * @param tls the TLS config with a certificate references
     * @return the virtual kafka cluster builder
     */
    public static VirtualKafkaClusterBuilder defaultVirtualKafkaClusterWithTlsCR(String namespaceName, String clusterName, String proxyName, String clusterRefName,
                                                                                 String ingressName, Tls tls) {
        //@formatter:off
        return baseVirtualKafkaClusterCR(namespaceName, clusterName, proxyName, clusterRefName, ingressName)
                .editSpec()
                    .editIngress(0)
                        .withNewTls()
                            .withCertificateRef(tls.getCertificateRef())
                        .endTls()
                    .endIngress()
                .endSpec();
        //@formatter:on
    }

    /**
     * Default virtual kafka cluster CR.
     *
     * @param namespaceName the namespace name
     * @param clusterName the cluster name
     * @param proxyName the proxy name
     * @param clusterRefName the cluster ref name
     * @param ingressName the ingress name
     * @param filterNames the filter names
     * @return the virtual kafka cluster builder
     */
    public static VirtualKafkaClusterBuilder virtualKafkaClusterWithFilterCR(String namespaceName, String clusterName, String proxyName, String clusterRefName,
                                                                             String ingressName, List<String> filterNames) {
        return baseVirtualKafkaClusterCR(namespaceName, clusterName, proxyName, clusterRefName, ingressName)
                .editSpec()
                .addAllToFilterRefs(getFilterRefs(filterNames))
                .endSpec();
    }

    private static Collection<FilterRef> getFilterRefs(List<String> filterNames) {
        List<FilterRef> filterRefs = new ArrayList<>();
        filterNames.forEach(filter -> filterRefs.add(new FilterRefBuilder().withName(filter).build()));
        return filterRefs;
    }
}
