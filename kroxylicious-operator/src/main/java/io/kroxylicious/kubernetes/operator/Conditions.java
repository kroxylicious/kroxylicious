/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
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

import edu.umd.cs.findbugs.annotations.NonNull;

public class Conditions {

    private final Condition condition;

    public Conditions(Condition condition) {
        this.condition = condition;
    }

    /**
     * Return new conditions by adding the given {@code conditionsToAdd} to the given {@code conditions},
     * keeping a single condition of each type.
     * When a {@code conditionToAdd} has a null status that condition will be removed.
     * @param conditions The existing conditions
     * @param conditionsToAdd The conditions to add
     * @return The new conditions.
     */
    static List<Condition> maybeAddOrUpdateConditions(List<Condition> conditions, List<Condition> conditionsToAdd) {
        Comparator<Condition> conditionComparator = Comparator
                .comparing(Condition::getType)
                .thenComparing(Comparator.comparing(Condition::getObservedGeneration).reversed())
                .thenComparing(Condition::getStatus)
                .thenComparing(Condition::getReason)
                .thenComparing(Condition::getMessage)
                .thenComparing(Condition::getLastTransitionTime);

        for (Condition condition : conditionsToAdd) {
            var type = Objects.requireNonNull(condition.getType());

            Map<Condition.Type, List<Condition>> byType = conditions.stream().collect(Collectors.groupingBy(
                    Condition::getType));

            var map = byType.entrySet().stream().map(entry -> {
                // minimum must exist because values of groupingBy result are always non-empty
                Condition minimumCondition = entry.getValue().stream().min(conditionComparator).orElseThrow();
                var mutableList = new ArrayList<Condition>(1);
                mutableList.add(minimumCondition);
                return Map.entry(entry.getKey(), mutableList);
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                    (l1, l2) -> {
                        throw new IllegalStateException();
                    },
                    TreeMap::new));

            if (condition.getStatus() == null) {
                map.remove(type);
            }
            else {
                ArrayList<Condition> conditionsOfType = map.computeIfAbsent(type, k -> new ArrayList<>());
                if (conditionsOfType.isEmpty()) {
                    conditionsOfType.add(condition);
                }
                else if (conditionsOfType.get(0).getObservedGeneration() <= condition.getObservedGeneration()) {
                    conditionsOfType.set(0, condition);
                }
            }
            conditions = map.values().stream().flatMap(Collection::stream).toList();
        }
        return conditions;
    }

    @SuppressWarnings("java:S4276") // BiConsumer<S, Long> is correct, because it's Long on the status classes
    private static <R extends CustomResource<?, S>, S> R newStatus(R ingress,
                                                                   Supplier<R> resourceSupplier,
                                                                   Supplier<S> statusSupplier,
                                                                   BiConsumer<S, List<Condition>> conditionSetter,
                                                                   BiConsumer<S, Long> observedGenerationSetter,
                                                                   List<Condition> conditions) {
        var result = resourceSupplier.get();
        result.setMetadata(new ObjectMetaBuilder()
                .withName(ResourcesUtil.name(ingress))
                .withNamespace(ResourcesUtil.namespace(ingress))
                .withUid(ResourcesUtil.uid(ingress))
                .build());
        S status = statusSupplier.get();
        conditionSetter.accept(status, conditions);
        observedGenerationSetter.accept(status, ingress.getMetadata().getGeneration());
        result.setStatus(status);
        return result;
    }

    @NonNull
    static VirtualKafkaCluster patchWithCondition(VirtualKafkaCluster cluster, List<Condition> conditions) {
        return newStatus(
                cluster,
                VirtualKafkaCluster::new,
                VirtualKafkaClusterStatus::new,
                VirtualKafkaClusterStatus::setConditions,
                VirtualKafkaClusterStatus::setObservedGeneration,
                maybeAddOrUpdateConditions(
                        Optional.of(cluster)
                                .map(VirtualKafkaCluster::getStatus)
                                .map(VirtualKafkaClusterStatus::getConditions)
                                .orElse(List.of()),
                        conditions));
    }

    @NonNull
    static KafkaService patchWithCondition(KafkaService service, Condition condition) {
        return newStatus(
                service,
                KafkaService::new,
                KafkaServiceStatus::new,
                KafkaServiceStatus::setConditions,
                KafkaServiceStatus::setObservedGeneration,
                maybeAddOrUpdateConditions(
                        Optional.of(service)
                                .map(KafkaService::getStatus)
                                .map(KafkaServiceStatus::getConditions)
                                .orElse(List.of()),
                        List.of(condition)));
    }

    @NonNull
    static KafkaProxy patchWithCondition(KafkaProxy proxy, Condition condition) {
        return newStatus(
                proxy,
                KafkaProxy::new,
                KafkaProxyStatus::new,
                KafkaProxyStatus::setConditions,
                KafkaProxyStatus::setObservedGeneration,
                maybeAddOrUpdateConditions(
                        Optional.of(proxy)
                                .map(KafkaProxy::getStatus)
                                .map(KafkaProxyStatus::getConditions)
                                .orElse(List.of()),
                        List.of(condition)));
    }

    @NonNull
    static KafkaProxyIngress patchWithCondition(KafkaProxyIngress ingress, Condition condition) {
        return newStatus(
                ingress,
                KafkaProxyIngress::new,
                KafkaProxyIngressStatus::new,
                KafkaProxyIngressStatus::setConditions,
                KafkaProxyIngressStatus::setObservedGeneration,
                maybeAddOrUpdateConditions(
                        Optional.of(ingress)
                                .map(KafkaProxyIngress::getStatus)
                                .map(KafkaProxyIngressStatus::getConditions)
                                .orElse(List.of()),
                        List.of(condition)));
    }

    @NonNull
    static KafkaProtocolFilter patchWithCondition(KafkaProtocolFilter filter, Condition condition) {
        return newStatus(
                filter,
                KafkaProtocolFilter::new,
                KafkaProtocolFilterStatus::new,
                KafkaProtocolFilterStatus::setConditions,
                KafkaProtocolFilterStatus::setObservedGeneration,
                maybeAddOrUpdateConditions(
                        Optional.of(filter)
                                .map(KafkaProtocolFilter::getStatus)
                                .map(KafkaProtocolFilterStatus::getConditions)
                                .orElse(List.of()),
                        List.of(condition)));
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

    static ErrorStatusUpdateControl<KafkaProtocolFilter> newUnknownConditionStatusPatch(Clock clock,
                                                                                        KafkaProtocolFilter observedResource,
                                                                                        Condition.Type type,
                                                                                        Exception e) {
        Condition unknownCondition = newConditionBuilder(clock, observedResource)
                .withType(type)
                .withStatus(Condition.Status.UNKNOWN)
                .withReason(e.getClass().getName())
                .withMessage(e.getMessage())
                .build();

        var newResource = Conditions.patchWithCondition(observedResource, unknownCondition);

        return ErrorStatusUpdateControl.patchStatus(newResource);
    }

    static ErrorStatusUpdateControl<KafkaProxyIngress> newUnknownConditionStatusPatch(Clock clock,
                                                                                      KafkaProxyIngress observedResource,
                                                                                      Condition.Type type,
                                                                                      Exception e) {
        Condition unknownCondition = newConditionBuilder(clock, observedResource)
                .withType(type)
                .withStatus(Condition.Status.UNKNOWN)
                .withReason(e.getClass().getName())
                .withMessage(e.getMessage())
                .build();

        var newResource = Conditions.patchWithCondition(observedResource, unknownCondition);

        return ErrorStatusUpdateControl.patchStatus(newResource);
    }

    static ErrorStatusUpdateControl<KafkaService> newUnknownConditionStatusPatch(Clock clock,
                                                                                 KafkaService observedResource,
                                                                                 Condition.Type type,
                                                                                 Exception e) {
        Condition unknownCondition = newConditionBuilder(clock, observedResource)
                .withType(type)
                .withStatus(Condition.Status.UNKNOWN)
                .withReason(e.getClass().getName())
                .withMessage(e.getMessage())
                .build();

        var newResource = Conditions.patchWithCondition(observedResource, unknownCondition);

        return ErrorStatusUpdateControl.patchStatus(newResource);
    }

    static ErrorStatusUpdateControl<KafkaProxy> newUnknownConditionStatusPatch(Clock clock,
                                                                               KafkaProxy observedResource,
                                                                               Condition.Type type,
                                                                               Exception e) {
        Condition unknownCondition = newConditionBuilder(clock, observedResource)
                .withType(type)
                .withStatus(Condition.Status.UNKNOWN)
                .withReason(e.getClass().getName())
                .withMessage(e.getMessage())
                .build();

        var newResource = Conditions.patchWithCondition(observedResource, unknownCondition);

        return ErrorStatusUpdateControl.patchStatus(newResource);
    }

    static ErrorStatusUpdateControl<VirtualKafkaCluster> newUnknownConditionStatusPatch(Clock clock,
                                                                                        VirtualKafkaCluster observedResource,
                                                                                        Condition.Type type,
                                                                                        Exception e) {
        Condition unknownCondition = newConditionBuilder(clock, observedResource)
                .withType(type)
                .withStatus(Condition.Status.UNKNOWN)
                .withReason(e.getClass().getName())
                .withMessage(e.getMessage())
                .build();

        var newResource = Conditions.patchWithCondition(observedResource, List.of(unknownCondition));

        return ErrorStatusUpdateControl.patchStatus(newResource);
    }

    public Conditions update(Condition condition) {
        if (condition.getObservedGeneration() >= this.condition.getObservedGeneration()) {
            return new Conditions(condition);
        }
        else {
            return this;
        }
    }

    public List<Condition> toList() {
        return List.of(condition);
    }
}
