/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.HasMetadata;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterStatus;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterStatus;

public class Conditions {

    public static final Comparator<Condition> STATE_TRANSITION_COMPARATOR = Comparator.comparing(Condition::getMessage)
            .thenComparing(Condition::getReason)
            .thenComparing(Condition::getStatus)
            .thenComparing(Condition::getType);
    private final Condition condition;

    private Conditions(Condition condition) {
        Objects.requireNonNull(condition, "condition cannot be null");
        this.condition = condition;
    }

    /**
     * Get a Conditions from the list of conditions on a CR's status.
     * @param conditionsList Conditions from the list of conditions on a CR's status.
     * @return An optional conditions object
     */
    static Optional<Conditions> fromList(List<Condition> conditionsList) {
        // Belt+braces: There _should_ be at most one such condition, but we assume there's more than one
        // we pick the condition with the largest observedGeneration (there's on point keeping old conditions around)
        // then we prefer Unknown over False over True statuses
        // finally we compare the last transition time, though this is only serialized with second resolution
        // and there's no guarantee that they call came from the same clock.
        return conditionsList.stream()
                .max(Comparator.comparing(Condition::getObservedGeneration)
                        .thenComparing(Condition::getStatus).reversed()
                        .thenComparing(Condition::getLastTransitionTime))
                .map(Conditions::new);
    }

    public static Conditions updateWith(Optional<Conditions> existingConditions, Condition condition) {
        return existingConditions.map(existing -> {
            if (condition.getObservedGeneration() == null) {
                return existing;
            }
            if (existing.condition.getObservedGeneration() == null) {
                return new Conditions(condition);
            }
            if (condition.getObservedGeneration() >= existing.condition.getObservedGeneration()) {
                return new Conditions(condition);
            }
            else {
                return existing;
            }
        }).orElse(new Conditions(condition));
    }

    public List<Condition> toList() {
        return List.of(condition);
    }

    private static List<Condition> newConditions(List<Condition> oldConditions, Condition newCondition) {
        Optional<Conditions> existingConditions = fromList(oldConditions);
        Conditions conditions = updateWith(existingConditions, newCondition);

        if (Condition.Status.TRUE == conditions.condition.getStatus()) {
            // True is the default status, so if the new condition would be True then return the empty list.
            return List.of();
        }
        if (existingConditions.isPresent()) {
            // If the two conditions are the same except for observedGeneration
            // and lastTransitionTime then update the new condition's lastTransitionTime
            // because it doesn't really represent a state transition
            var existing = existingConditions.get();
            if (STATE_TRANSITION_COMPARATOR.compare(existing.condition, conditions.condition) == 0
                    && existing.condition.getLastTransitionTime() != null) {
                conditions.condition.setLastTransitionTime(existing.condition.getLastTransitionTime());
            }
        }
        return conditions.toList();
    }

    static ConditionBuilder newConditionBuilder(Clock clock, HasMetadata observedGenerationSource) {
        var now = clock.instant();
        return new ConditionBuilder()
                .withLastTransitionTime(now)
                .withObservedGeneration(observedGenerationSource.getMetadata().getGeneration());
    }

    static Condition newTrueCondition(Clock clock, HasMetadata observedGenerationSource, Condition.Type type) {
        return newConditionBuilder(clock, observedGenerationSource)
                .withType(type)
                .withStatus(Condition.Status.TRUE)
                .build();
    }

    static Condition newFalseCondition(Clock clock,
                                       HasMetadata observedGenerationSource,
                                       Condition.Type type,
                                       String reason,
                                       String message) {
        return newConditionBuilder(clock, observedGenerationSource)
                .withType(type)
                .withStatus(Condition.Status.FALSE)
                .withReason(reason)
                .withMessage(message)
                .build();
    }

    private static Condition newUnknownCondition(Clock clock, HasMetadata observedResource, Condition.Type type, Exception e) {
        return newConditionBuilder(clock, observedResource)
                .withType(type)
                .withStatus(Condition.Status.UNKNOWN)
                .withReason(e.getClass().getName())
                .withMessage(e.getMessage())
                .build();
    }

    private static KafkaProxy kafkaProxyStatusPatch(KafkaProxy observedProxy,
                                               Condition unknownCondition) {
        // @formatter:off
        return new KafkaProxyBuilder()
                .withNewMetadata()
                    .withUid(ResourcesUtil.uid(observedProxy))
                    .withName(ResourcesUtil.name(observedProxy))
                    .withNamespace(ResourcesUtil.namespace(observedProxy))
                .endMetadata()
                .withNewStatus()
                    .withObservedGeneration(ResourcesUtil.generation(observedProxy))
                    .withConditions(newConditions(Optional.ofNullable(observedProxy.getStatus()).map(KafkaProxyStatus::getConditions).orElse(List.of()), unknownCondition))
                .endStatus()
                .build();
        // @formatter:on
    }

    static KafkaProxy newUnknownConditionStatusPatch(Clock clock,
                                                     KafkaProxy observedProxy,
                                                     Condition.Type type,
                                                     Exception e) {
        Condition unknownCondition = newUnknownCondition(clock, observedProxy, type, e);
        return kafkaProxyStatusPatch(observedProxy, unknownCondition);
    }

    static KafkaProxy newFalseConditionStatusPatch(Clock clock,
                                                   KafkaProxy observedProxy,
                                                   Condition.Type type,
                                                   String reason,
                                                   String message) {
        Condition falseCondition = newFalseCondition(clock, observedProxy, type, reason, message);
        return kafkaProxyStatusPatch(observedProxy, falseCondition);
    }

    static KafkaProxy newTrueConditionStatusPatch(Clock clock,
                                                  KafkaProxy observedProxy,
                                                  Condition.Type type) {
        Condition trueCondition = newTrueCondition(clock, observedProxy, type);
        return kafkaProxyStatusPatch(observedProxy, trueCondition);
    }

    private static KafkaProtocolFilter filterStatusPatch(KafkaProtocolFilter observedProxy,
                                                         Condition unknownCondition) {
        // @formatter:off
        return new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                    .withUid(ResourcesUtil.uid(observedProxy))
                    .withName(ResourcesUtil.name(observedProxy))
                    .withNamespace(ResourcesUtil.namespace(observedProxy))
                .endMetadata()
                .withNewStatus()
                    .withObservedGeneration(ResourcesUtil.generation(observedProxy))
                    .withConditions(newConditions(Optional.ofNullable(observedProxy.getStatus()).map(KafkaProtocolFilterStatus::getConditions).orElse(List.of()), unknownCondition))
                .endStatus()
                .build();
        // @formatter:on
    }

    static KafkaProtocolFilter newUnknownConditionStatusPatch(Clock clock,
                                                              KafkaProtocolFilter observedFilter,
                                                              Condition.Type type,
                                                              Exception e) {
        Condition unknownCondition = newUnknownCondition(clock, observedFilter, type, e);
        return filterStatusPatch(observedFilter, unknownCondition);
    }

    static KafkaProtocolFilter newFalseConditionStatusPatch(Clock clock,
                                                            KafkaProtocolFilter observedProxy,
                                                            Condition.Type type,
                                                            String reason,
                                                            String message) {
        Condition falseCondition = newFalseCondition(clock, observedProxy, type, reason, message);
        return filterStatusPatch(observedProxy, falseCondition);
    }

    static KafkaProtocolFilter newTrueConditionStatusPatch(Clock clock,
                                                           KafkaProtocolFilter observedProxy,
                                                           Condition.Type type) {
        Condition trueCondition = newTrueCondition(clock, observedProxy, type);
        return filterStatusPatch(observedProxy, trueCondition);
    }

    private static KafkaProxyIngress ingressStatusPatch(KafkaProxyIngress observedIngress,
                                                        Condition unknownCondition) {
        // @formatter:off
        return new KafkaProxyIngressBuilder()
                .withNewMetadata()
                    .withUid(ResourcesUtil.uid(observedIngress))
                    .withName(ResourcesUtil.name(observedIngress))
                    .withNamespace(ResourcesUtil.namespace(observedIngress))
                .endMetadata()
                .withNewStatus()
                    .withObservedGeneration(ResourcesUtil.generation(observedIngress))
                    .withConditions(newConditions(Optional.ofNullable(observedIngress.getStatus()).map(KafkaProxyIngressStatus::getConditions).orElse(List.of()), unknownCondition))
                .endStatus()
                .build();
        // @formatter:on
    }

    static KafkaProxyIngress newUnknownConditionStatusPatch(Clock clock,
                                                            KafkaProxyIngress observedFilter,
                                                            Condition.Type type,
                                                            Exception e) {
        Condition unknownCondition = newUnknownCondition(clock, observedFilter, type, e);
        return ingressStatusPatch(observedFilter, unknownCondition);
    }

    static KafkaProxyIngress newFalseConditionStatusPatch(Clock clock,
                                                          KafkaProxyIngress observedProxy,
                                                          Condition.Type type,
                                                          String reason,
                                                          String message) {
        Condition falseCondition = newFalseCondition(clock, observedProxy, type, reason, message);
        return ingressStatusPatch(observedProxy, falseCondition);
    }

    static KafkaProxyIngress newTrueConditionStatusPatch(Clock clock,
                                                         KafkaProxyIngress observedProxy,
                                                         Condition.Type type) {
        Condition trueCondition = newTrueCondition(clock, observedProxy, type);
        return ingressStatusPatch(observedProxy, trueCondition);
    }

    private static KafkaService serviceStatusPatch(KafkaService observedIngress,
                                                   Condition unknownCondition) {
        // @formatter:off
        return new KafkaServiceBuilder()
                .withNewMetadata()
                    .withUid(ResourcesUtil.uid(observedIngress))
                    .withName(ResourcesUtil.name(observedIngress))
                    .withNamespace(ResourcesUtil.namespace(observedIngress))
                .endMetadata()
                .withNewStatus()
                    .withObservedGeneration(ResourcesUtil.generation(observedIngress))
                    .withConditions(newConditions(Optional.ofNullable(observedIngress.getStatus()).map(KafkaServiceStatus::getConditions).orElse(List.of()), unknownCondition))
                .endStatus()
                .build();
        // @formatter:on
    }

    static KafkaService newUnknownConditionStatusPatch(Clock clock,
                                                       KafkaService observedFilter,
                                                       Condition.Type type,
                                                       Exception e) {
        Condition unknownCondition = newUnknownCondition(clock, observedFilter, type, e);
        return serviceStatusPatch(observedFilter, unknownCondition);
    }

    static KafkaService newFalseConditionStatusPatch(Clock clock,
                                                     KafkaService observedProxy,
                                                     Condition.Type type,
                                                     String reason,
                                                     String message) {
        Condition falseCondition = newFalseCondition(clock, observedProxy, type, reason, message);
        return serviceStatusPatch(observedProxy, falseCondition);
    }

    static KafkaService newTrueConditionStatusPatch(Clock clock,
                                                    KafkaService observedProxy,
                                                    Condition.Type type) {
        Condition trueCondition = newTrueCondition(clock, observedProxy, type);
        return serviceStatusPatch(observedProxy, trueCondition);
    }

    private static VirtualKafkaCluster clusterStatusPatch(VirtualKafkaCluster observedIngress,
                                                          Condition unknownCondition) {
        // @formatter:off
        return new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withUid(ResourcesUtil.uid(observedIngress))
                    .withName(ResourcesUtil.name(observedIngress))
                    .withNamespace(ResourcesUtil.namespace(observedIngress))
                .endMetadata()
                .withNewStatus()
                    .withObservedGeneration(ResourcesUtil.generation(observedIngress))
                    .withConditions(newConditions(Optional.ofNullable(observedIngress.getStatus()).map(VirtualKafkaClusterStatus::getConditions).orElse(List.of()), unknownCondition))
                .endStatus()
                .build();
        // @formatter:on
    }

    static VirtualKafkaCluster newUnknownConditionStatusPatch(Clock clock,
                                                              VirtualKafkaCluster observedFilter,
                                                              Condition.Type type,
                                                              Exception e) {
        Condition unknownCondition = newUnknownCondition(clock, observedFilter, type, e);
        return clusterStatusPatch(observedFilter, unknownCondition);
    }

    static VirtualKafkaCluster newFalseConditionStatusPatch(Clock clock,
                                                            VirtualKafkaCluster observedProxy,
                                                            Condition.Type type,
                                                            String reason,
                                                            String message) {
        Condition falseCondition = newFalseCondition(clock, observedProxy, type, reason, message);
        return clusterStatusPatch(observedProxy, falseCondition);
    }

    static VirtualKafkaCluster newTrueConditionStatusPatch(Clock clock,
                                                           VirtualKafkaCluster observedProxy,
                                                           Condition.Type type) {
        Condition trueCondition = newTrueCondition(clock, observedProxy, type);
        return clusterStatusPatch(observedProxy, trueCondition);
    }

}
