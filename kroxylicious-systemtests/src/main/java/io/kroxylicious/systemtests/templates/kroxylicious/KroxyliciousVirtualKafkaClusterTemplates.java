/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.templates.kroxylicious;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import io.kroxylicious.kubernetes.api.common.FilterRef;
import io.kroxylicious.kubernetes.api.common.FilterRefBuilder;
import io.kroxylicious.kubernetes.api.common.KafkaServiceRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.systemtests.Constants;

public class KroxyliciousVirtualKafkaClusterTemplates {

    private KroxyliciousVirtualKafkaClusterTemplates() {
    }

    private static VirtualKafkaClusterBuilder baseVirtualKafkaClusterCR(String clusterName, String ingressName) {
        // @formatter:off
        return new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withName(clusterName)
                .endMetadata()
                .withNewSpec()
                    .withTargetKafkaServiceRef(new KafkaServiceRefBuilder()
                            .withName(clusterName)
                            .build())
                    .withNewProxyRef()
                        .withName(Constants.KROXYLICIOUS_PROXY_SIMPLE_NAME)
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
     * Default virtual Kafka cluster CR.
     *
     * @param clusterName the cluster name
     * @param ingressName the ingress name
     * @return the virtual Kafka cluster builder
     */
    public static VirtualKafkaClusterBuilder defaultVirtualKafkaClusterCR(String clusterName, String ingressName) {
        return baseVirtualKafkaClusterCR(clusterName, ingressName);
    }

    /**
     * Default virtual Kafka cluster CR.
     *
     * @param clusterName the cluster name
     * @param ingressName the ingress name
     * @param filterNames the filter names
     * @return the virtual Kafka cluster builder
     */
    public static VirtualKafkaClusterBuilder virtualKafkaClusterWithFilterCR(String clusterName, String ingressName, List<String> filterNames) {
        return baseVirtualKafkaClusterCR(clusterName, ingressName)
                .editSpec()
                .addAllToFilterRefs(getFilterRefs(filterNames))
                .endSpec();
    }

    /**
     * Virtual Kafka cluster CR with filters and a TLS subject builder that extracts
     * the CN from the client certificate subject and maps it to a {@code User} principal.
     * Combine this with {@code KroxyliciousBuilder.withDownstreamTls(tls)} to get mTLS
     * with subject-based authorization.
     *
     * @param clusterName the cluster name
     * @param ingressName the ingress name
     * @param filterNames the filter names
     * @return the virtual Kafka cluster builder
     */
    public static VirtualKafkaClusterBuilder virtualKafkaClusterWithTlsSubjectBuilder(String clusterName, String ingressName, List<String> filterNames) {
        // @formatter:off
        return virtualKafkaClusterWithFilterCR(clusterName, ingressName, filterNames)
                .editSpec()
                    .withNewTlsSubjectBuilder()
                        .withType("DefaultTransportSubjectBuilderService")
                        .withNewConfigTemplate()
                            .addToAdditionalProperties("addPrincipals", List.of(Map.of(
                                    "from", "clientTlsSubject",
                                    "map", List.of(
                                            Map.of("replaceMatch", "/(?:.*,)?CN=([^,]+)(?:,.*)?/$1/"),
                                            Map.of("else", "anonymous")),
                                    "principalFactory", "io.kroxylicious.proxy.authentication.UserFactory")))
                        .endConfigTemplate()
                    .endTlsSubjectBuilder()
                .endSpec();
        // @formatter:on
    }

    private static Collection<FilterRef> getFilterRefs(List<String> filterNames) {
        List<FilterRef> filterRefs = new ArrayList<>();
        filterNames.forEach(filter -> filterRefs.add(new FilterRefBuilder().withName(filter).build()));
        return filterRefs;
    }
}
