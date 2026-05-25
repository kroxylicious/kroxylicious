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

    private static Collection<FilterRef> getFilterRefs(List<String> filterNames) {
        List<FilterRef> filterRefs = new ArrayList<>();
        filterNames.forEach(filter -> filterRefs.add(new FilterRefBuilder().withName(filter).build()));
        return filterRefs;
    }
}
