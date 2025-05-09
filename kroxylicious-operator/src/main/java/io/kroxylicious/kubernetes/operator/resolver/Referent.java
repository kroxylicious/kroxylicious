/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.List;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.HasMetadata;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterStatus;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterStatus;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

/**
 * A single referent
 * @param resource resource
 * @param isStale true iff the status.observedGeneration does not equal the metadata.generation of the resource
 * @param hasResolvedRefsFalseCondition true iff any of the resources status conditions is ResolvedRefs=False
 * @param <T> referent type
 */
public record Referent<T extends HasMetadata>(T resource, boolean isStale, boolean hasResolvedRefsFalseCondition) {

    public static Referent<KafkaProtocolFilter> from(KafkaProtocolFilter filter) {
        boolean resolvedRefsFalse = hasAnyResolvedRefsFalse(
                Optional.ofNullable(filter.getStatus()).map(KafkaProtocolFilterStatus::getConditions).orElse(List.of()));
        return new Referent<>(filter, !ResourcesUtil.isStatusFresh(filter), resolvedRefsFalse);
    }

    public static Referent<VirtualKafkaCluster> from(VirtualKafkaCluster cluster) {
        boolean resolvedRefsFalse = hasAnyResolvedRefsFalse(
                Optional.ofNullable(cluster.getStatus()).map(VirtualKafkaClusterStatus::getConditions).orElse(List.of()));
        return new Referent<>(cluster, !ResourcesUtil.isStatusFresh(cluster), resolvedRefsFalse);
    }

    public static Referent<KafkaService> from(KafkaService service) {
        boolean resolvedRefsFalse = hasAnyResolvedRefsFalse(
                Optional.ofNullable(service.getStatus()).map(KafkaServiceStatus::getConditions).orElse(List.of()));
        return new Referent<>(service, !ResourcesUtil.isStatusFresh(service), resolvedRefsFalse);
    }

    public static Referent<KafkaProxy> from(KafkaProxy proxy) {
        boolean resolvedRefsFalse = hasAnyResolvedRefsFalse(
                Optional.ofNullable(proxy.getStatus()).map(KafkaProxyStatus::getConditions).orElse(List.of()));
        return new Referent<>(proxy, false, resolvedRefsFalse);
    }

    public static Referent<KafkaProxyIngress> from(KafkaProxyIngress ingress) {
        boolean resolvedRefsFalse = hasAnyResolvedRefsFalse(
                Optional.ofNullable(ingress.getStatus()).map(KafkaProxyIngressStatus::getConditions).orElse(List.of()));
        return new Referent<>(ingress, !ResourcesUtil.isStatusFresh(ingress), resolvedRefsFalse);
    }

    private static boolean hasAnyResolvedRefsFalse(List<Condition> conditions) {
        return conditions.stream().anyMatch(Condition::isResolvedRefsFalse);
    }

}
