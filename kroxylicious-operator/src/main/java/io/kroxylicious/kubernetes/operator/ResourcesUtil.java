/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.common.AnyLocalRefBuilder;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterStatus;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterStatus;

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

    public static String name(HasMetadata resource) {
        return resource.getMetadata().getName();
    }

    public static String namespace(HasMetadata resource) {
        return resource.getMetadata().getNamespace();
    }

    /**
     * Extract generation from a resource's {@code metadata} object.
     *
     * @param resource the object from which to extract the metadata generation.
     * @return the metadata generation of <code>0</code> if the metadata or the generation itself is null in alignment with @see <a href="https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/1623-standardize-conditions#kep-1623-standardize-conditions">KEP 1623</a>.
     */
    public static long generation(HasMetadata resource) {
        ObjectMeta metadata = resource.getMetadata();
        if (metadata.getGeneration() == null) {
            return 0L;
        }
        return metadata.getGeneration();
    }

    public static String uid(HasMetadata resource) {
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
    public static <T extends HasMetadata> Optional<T> findOnlyResourceNamed(String name, Collection<T> collection) {
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
    public static <T extends HasMetadata> Collector<T, ?, Map<String, T>> toByNameMap() {
        return Collectors.toMap(ResourcesUtil::name, Function.identity());
    }

    /**
     * Collector that collects elements of stream to a map keyed by the local ref for that element
     *
     * @param <T> resource type
     * @return a Collector that collects a Map from element name to element
     */
    public static <T extends HasMetadata> Collector<T, ?, Map<LocalRef<T>, T>> toByLocalRefMap() {
        return Collectors.toMap(ResourcesUtil::toLocalRef, Function.identity());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T extends HasMetadata> LocalRef<T> toLocalRef(T ref) {
        return (LocalRef) new AnyLocalRefBuilder()
                .withKind(ref.getKind())
                .withGroup(group(ref))
                .withName(name(ref))
                .build();
    }

    public static String group(HasMetadata resource) {
        // core CustomResource classes like Secret, Deployment etc. have a group of empty string and their apiVersion is the String 'v1'
        if (!resource.getApiVersion().contains("/")) {
            return "";
        }
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

    static <T> boolean isReferent(LocalRef<T> ref, HasMetadata resource) {
        return Objects.equals(ResourcesUtil.name(resource), ref.getName());
    }

    /**
     * Converts a {@code ref}, held by the given {@code owner}, into the equivalent ResourceID
     * @param owner The owner of the reference
     * @param ref The reference held by the owner
     * @return A singleton ResourceID
     * @param <O> The type of the reference owner
     * @param <R> The type of the referent
     */
    static <O extends HasMetadata, R extends HasMetadata> Set<ResourceID> localRefAsResourceId(O owner, LocalRef<R> ref) {
        return Set.of(new ResourceID(ref.getName(), owner.getMetadata().getNamespace()));
    }

    static <O extends HasMetadata, R extends HasMetadata> Set<ResourceID> localRefsAsResourceIds(O owner,
                                                                                                 List<? extends LocalRef<R>> refs) {
        return refs.stream()
                .map(ref -> new ResourceID(ref.getName(), owner.getMetadata().getNamespace()))
                .collect(Collectors.toSet());
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
     * Like {@link #findReferrers(EventSourceContext, HasMetadata, Class, Function)}
     * except for the case where the owner is able to reference multiple referents (i.e. {@code refAccessor} returns a Collection.
     * @param context The context
     * @param referent The potential referent
     * @param owner The type of the owner of the reference
     * @param refAccessor A function which returns the references from a given owner.
     * @return The ids of reference owners which refer to the referent.
     * @param <O> The type of the reference owner
     * @param <R> The type of the referent
     */
    static <O extends HasMetadata, R extends HasMetadata> Set<ResourceID> findReferrersMulti(EventSourceContext<?> context,
                                                                                             R referent,
                                                                                             Class<O> owner,
                                                                                             Function<O, Collection<? extends LocalRef<R>>> refAccessor) {
        return ResourcesUtil.filteredResourceIdsInSameNamespace(context,
                referent,
                owner,
                primary -> refAccessor.apply(primary).stream().anyMatch(ref -> ResourcesUtil.isReferent(ref, referent)));
    }

    public static String namespacedSlug(LocalRef<?> ref, HasMetadata resource) {
        return slug(ref) + " in namespace '" + namespace(resource) + "'";
    }

    private static String slug(LocalRef<?> ref) {
        String group = ref.getGroup();
        String name = ref.getName();
        String groupString = group.isEmpty() ? "" : "." + group;
        return ref.getKind().toLowerCase(Locale.ROOT) + groupString + "/" + name;
    }

    /**
     * Checks that the status observedGeneration is equal to the metadata generation. Indicating
     * that the current {@code spec} of the resource has been reconciled.
     * @param cluster cluster
     * @return true if status observedGeneration is equal to the metadata generation
     */
    public static boolean isStatusFresh(VirtualKafkaCluster cluster) {
        return isStatusFresh(cluster, c -> Optional.ofNullable(c.getStatus()).map(VirtualKafkaClusterStatus::getObservedGeneration).orElse(null));
    }

    /**
     * Checks that the status observedGeneration is equal to the metadata generation. Indicating
     * that the current {@code spec} of the resource has been reconciled.
     * @param ingress ingress
     * @return true if status observedGeneration is equal to the metadata generation
     */
    public static boolean isStatusFresh(KafkaProxyIngress ingress) {
        return isStatusFresh(ingress, i -> Optional.ofNullable(i.getStatus()).map(KafkaProxyIngressStatus::getObservedGeneration).orElse(null));
    }

    /**
     * Checks that the status observedGeneration is equal to the metadata generation. Indicating
     * that the current {@code spec} of the resource has been reconciled.
     * @param service service
     * @return true if status observedGeneration is equal to the metadata generation
     */
    public static boolean isStatusFresh(KafkaService service) {
        return isStatusFresh(service, i -> Optional.ofNullable(i.getStatus()).map(KafkaServiceStatus::getObservedGeneration).orElse(null));
    }

    /**
     * Checks that the status observedGeneration is equal to the metadata generation. Indicating
     * that the current {@code spec} of the resource has been reconciled.
     * @param filter filter
     * @return true if status observedGeneration is equal to the metadata generation
     */
    public static boolean isStatusFresh(KafkaProtocolFilter filter) {
        return isStatusFresh(filter, i -> Optional.ofNullable(i.getStatus()).map(KafkaProtocolFilterStatus::getObservedGeneration).orElse(null));
    }

    private static <T extends HasMetadata> boolean isStatusFresh(T resource, Function<T, Long> observedGenerationFunc) {
        Long observedGeneration = observedGenerationFunc.apply(resource);
        Long generation = resource.getMetadata().getGeneration();
        return Objects.equals(generation, observedGeneration);
    }
}
