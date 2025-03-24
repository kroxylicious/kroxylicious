/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.umd.cs.findbugs.annotations.Nullable;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.common.AnyLocalRefBuilder;
import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterStatus;

import edu.umd.cs.findbugs.annotations.NonNull;

public class ResourcesUtil {
    private ResourcesUtil() {
    }

    private static boolean inRange(char ch, char start, char end) {
        return start <= ch && ch <= end;
    }

    private static boolean isAlnum(char ch) {
        return inRange(ch, 'a', 'z')
                || inRange(ch, '0', '9');
    }

    static boolean isDnsLabel(String string, boolean rfc1035) {
        int length = string.length();
        if (length == 0 || length > 63) {
            return false;
        }
        var ch = string.charAt(0);
        if (!(rfc1035 ? inRange(ch, 'a', 'z') : isAlnum(ch))) {
            return false;
        }
        if (length > 1) {
            if (length > 2) {
                for (int index = 1; index < length - 1; index++) {
                    ch = string.charAt(index);
                    if (!(isAlnum(ch) || ch == '-')) {
                        return false;
                    }
                }
            }
            ch = string.charAt(length - 1);
            return isAlnum(ch);
        }
        return true;
    }

    static String requireIsDnsLabel(String string, boolean rfc1035, String message) {
        if (!isDnsLabel(string, rfc1035)) {
            throw new IllegalArgumentException(message);
        }
        return string;
    }

    public static <O extends HasMetadata> OwnerReference newOwnerReferenceTo(O owner) {
        return new OwnerReferenceBuilder()
                .withKind(owner.getKind())
                .withApiVersion(owner.getApiVersion())
                .withName(name(owner))
                .withUid(uid(owner))
                .build();
    }

    public static Stream<VirtualKafkaCluster> clustersInNameOrder(Context<KafkaProxy> context) {
        return context.getSecondaryResources(VirtualKafkaCluster.class)
                .stream()
                .sorted(Comparator.comparing(ResourcesUtil::name));
    }

    public static String name(@NonNull HasMetadata resource) {
        return resource.getMetadata().getName();
    }

    public static String namespace(@NonNull HasMetadata resource) {
        return resource.getMetadata().getNamespace();
    }

    public static Long generation(@NonNull HasMetadata resource) {
        return resource.getMetadata().getGeneration();
    }

    public static String uid(@NonNull HasMetadata resource) {
        return resource.getMetadata().getUid();
    }

    /**
     * Find the only element in the collection with metadata.name matching the search name.
     *
     * @param <T> resource type
     * @param name name to search for
     * @param collection collection to search
     * @return an Optional containing the only element matching, empty if there is no matching element
     * @throws IllegalStateException if there are multiple elements matching
     */
    public static <T extends HasMetadata> Optional<T> findOnlyResourceNamed(@NonNull String name, @NonNull Collection<T> collection) {
        Objects.requireNonNull(collection);
        Objects.requireNonNull(name);
        List<T> list = collection.stream().filter(item -> name(item).equals(name)).toList();
        if (list.size() > 1) {
            throw new IllegalStateException("collection contained more than one resource named " + name);
        }
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
    }

    /**
     * Collector that collects elements of stream to a map keyed by name of the element
     *
     * @param <T> resource type
     * @return a Collector that collects a Map from element name to element
     */
    public static <T extends HasMetadata> @NonNull Collector<T, ?, Map<String, T>> toByNameMap() {
        return Collectors.toMap(ResourcesUtil::name, Function.identity());
    }

    /**
     * Collector that collects elements of stream to a map keyed by the local ref for that element
     *
     * @param <T> resource type
     * @return a Collector that collects a Map from element name to element
     */
    public static <T extends HasMetadata> @NonNull Collector<T, ?, Map<LocalRef<T>, T>> toByLocalRefMap() {
        return Collectors.toMap(ResourcesUtil::toLocalRef, Function.identity());
    }

    public static <T extends HasMetadata> LocalRef<T> toLocalRef(T ref) {
        return (LocalRef) new AnyLocalRefBuilder()
                .withKind(ref.getKind())
                .withGroup(group(ref))
                .withName(name(ref))
                .build();
    }

    public static String group(HasMetadata resource) {
        return resource.getApiVersion().substring(0, resource.getApiVersion().indexOf("/"));
    }

    static <T extends HasMetadata> Set<ResourceID> filteredResourceIdsInSameNamespace(EventSourceContext<?> context,
                                                                                      HasMetadata primary,
                                                                                      Class<T> clazz,
                                                                                      Predicate<T> predicate) {
        return resourcesInSameNamespace(context, primary, clazz)
                .filter(predicate)
                .map(ResourceID::fromResource)
                .collect(Collectors.toSet());
    }

    /**
     * Get the resources of the given clazz that are in the same namespace as the given primary.
     * @param context The context
     * @param primary The primary
     * @param clazz The class of resources to get
     * @return A stream of resources
     * @param <T> The type of the resource
     */
    static <T extends HasMetadata> Stream<T> resourcesInSameNamespace(EventSourceContext<?> context, HasMetadata primary, Class<T> clazz) {
        return context.getClient()
                .resources(clazz)
                .inNamespace(namespace(primary))
                .list()
                .getItems()
                .stream();
    }

    static <T> boolean isReferent(LocalRef<T> ref, HasMetadata proxy) {
        return Objects.equals(ResourcesUtil.name(proxy), ref.getName());
    }

    /**
     * Converts a {@code ref}, held by the given {@code owner}, into the equivalent ResourceID
     * @param owner The owner of the reference
     * @param ref The reference held by the owner
     * @return A singleton ResourceID
     * @param <O> The type of the reference owner
     * @param <R> The type of the referent
     */
    @NonNull
    static <O extends HasMetadata, R extends HasMetadata> Set<ResourceID> localRefAsResourceId(O owner, LocalRef<R> ref) {
        return Set.of(new ResourceID(ref.getName(), owner.getMetadata().getNamespace()));
    }

    /**
     * Finds the (ids of) the resources which reference the given referent
     * This is the inverse of {@link #localRefAsResourceId(HasMetadata, LocalRef)}}.
     * @param context The context
     * @param referent The referent
     * @param owner The type of the owner of the reference
     * @param refAccessor A function which returns the reference from a given owner.
     * @return The ids of reference owners which refer to the referent.
     * @param <O> The type of the reference owner
     * @param <R> The type of the referent
     */
    @NonNull
    static <O extends HasMetadata, R extends HasMetadata> Set<ResourceID> findReferrers(EventSourceContext<?> context,
                                                                                        R referent,
                                                                                        Class<O> owner,
                                                                                        Function<O, LocalRef<R>> refAccessor) {
        return ResourcesUtil.filteredResourceIdsInSameNamespace(context,
                referent,
                owner,
                primary -> ResourcesUtil.isReferent(refAccessor.apply(primary), referent));
    }

    /**
     * <p>Return an updated list of conditions which:</p>
     * <ul>
     * <li>Preserves the order of conditions with a different type than the given condition.</li>
     * <li>Includes exactly one condition of the same type as the given condition.</li>
     * <li>Includes the given condition at index 0 if there are no existing conditions of the same type.</li>
     * <li>Otherwise includes the given condition if it represents a state transition from all existing conditions of the same type.</li>
     * </ul>
     * <p>These semantics mean callers of this method don't have to assume that conditions of other (possibly unknown)
     * types can't occur more than once, and also re-ordering of the list</p>
     * @param conditions
     * @param condition
     * @return A new list
     */
    static List<Condition> maybeAddOrUpdateCondition(List<Condition> conditions, Condition condition) {
        var type = Objects.requireNonNull(condition.getType());

        Comparator<Condition> conditionComparator = Comparator
                .comparing(Condition::getType)
                .thenComparing(Comparator.comparing(Condition::getObservedGeneration).reversed())
                .thenComparing(Condition::getStatus)
                .thenComparing(Condition::getReason)
                .thenComparing(Condition::getMessage)
                .thenComparing(Condition::getLastTransitionTime);

        TreeMap<Condition.Type, ArrayList<Condition>> byType = conditions.stream().collect(Collectors.groupingBy(
                Condition::getType,
                TreeMap::new, // order based on Type
                Collectors.toCollection(ArrayList::new)));

        byType.computeIfAbsent(type, k -> new ArrayList<>()).add(condition);

        var map = byType.entrySet().stream().map(entry -> {
            var conditionsOfType = entry.getValue();

            return Map.entry(entry.getKey(), List.of(conditionsOfType.stream().sorted(conditionComparator).findFirst().get()));
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                (l1, l2) -> {
                    throw new IllegalStateException();
                },
                TreeMap::new));

        return map.values().stream().flatMap(Collection::stream).toList();
    }

    private static <R extends CustomResource<?, S>, S> R newStatusG(R ingress,
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
    static KafkaService patchWithCondition(KafkaService service, Condition condition) {
        return newStatusG(
                service,
                KafkaService::new,
                KafkaServiceStatus::new,
                KafkaServiceStatus::setConditions,
                KafkaServiceStatus::setObservedGeneration,
                maybeAddOrUpdateCondition(
                        Optional.of(service)
                                .map(KafkaService::getStatus)
                                .map(KafkaServiceStatus::getConditions)
                                .orElse(List.of()),
                        condition));
    }

    @NonNull
    static KafkaProxyIngress patchWithCondition(KafkaProxyIngress ingress, Condition condition) {
        return newStatusG(
                ingress,
                KafkaProxyIngress::new,
                KafkaProxyIngressStatus::new,
                KafkaProxyIngressStatus::setConditions,
                KafkaProxyIngressStatus::setObservedGeneration,
                maybeAddOrUpdateCondition(
                        Optional.of(ingress)
                                .map(KafkaProxyIngress::getStatus)
                                .map(KafkaProxyIngressStatus::getConditions)
                                .orElse(List.of()),
                        condition));
    }

    @NonNull
    static KafkaProtocolFilter patchWithCondition(KafkaProtocolFilter filter, Condition condition) {
        return newStatusG(
                filter,
                KafkaProtocolFilter::new,
                KafkaProtocolFilterStatus::new,
                KafkaProtocolFilterStatus::setConditions,
                KafkaProtocolFilterStatus::setObservedGeneration,
                maybeAddOrUpdateCondition(
                        Optional.of(filter)
                                .map(KafkaProtocolFilter::getStatus)
                                .map(KafkaProtocolFilterStatus::getConditions)
                                .orElse(List.of()),
                        condition));
    }

}
