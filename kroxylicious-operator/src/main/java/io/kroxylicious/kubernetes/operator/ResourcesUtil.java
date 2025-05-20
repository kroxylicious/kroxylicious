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

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.CustomResource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.common.AnyLocalRefBuilder;
import io.kroxylicious.kubernetes.api.common.CertificateRef;
import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRef;
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

import static io.kroxylicious.kubernetes.api.common.Condition.Type.ResolvedRefs;

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

    static String volumeName(String group, String plural, String resourceName) {
        String volumeNamePrefix = group.isEmpty() ? plural : group + "." + plural;
        String volumeName = volumeNamePrefix + "-" + resourceName;
        ResourcesUtil.requireIsDnsLabel(volumeName, true,
                "volume name would not be a DNS label: " + volumeName);
        return volumeName;
    }

    static boolean isSecret(LocalRef<?> ref) {
        return (ref.getKind() == null || ref.getKind().isEmpty() || "Secret".equals(ref.getKind()))
                && (ref.getGroup() == null || ref.getGroup().isEmpty());
    }

    static boolean isConfigMap(LocalRef<?> ref) {
        return (ref.getKind() == null || ref.getKind().isEmpty() || "ConfigMap".equals(ref.getKind()))
                && (ref.getGroup() == null || ref.getGroup().isEmpty());
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
                                                                                        Function<O, Optional<LocalRef<R>>> refAccessor) {
        return ResourcesUtil.filteredResourceIdsInSameNamespace(context,
                referent,
                owner,
                primary -> refAccessor.apply(primary).map(lr -> ResourcesUtil.isReferent(lr, referent)).orElse(false));
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
                primary -> {
                    Collection<? extends LocalRef<R>> refs = refAccessor.apply(primary);
                    if (refs == null) {
                        return false;
                    }
                    return refs.stream().anyMatch(ref -> ResourcesUtil.isReferent(ref, referent));
                });
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
     * @param hasMetadata hasMetadata
     * @throws IllegalStateException if hasMetadata is not a CustomResource type owned by the Kroxylicious Operator
     * @return true if status observedGeneration is equal to the metadata generation
     */
    public static boolean isStatusFresh(HasMetadata hasMetadata) {
        Objects.requireNonNull(hasMetadata);
        if (hasMetadata instanceof KafkaProtocolFilter filter) {
            return isStatusFresh(filter, i -> Optional.ofNullable(i.getStatus()).map(KafkaProtocolFilterStatus::getObservedGeneration).orElse(null));
        }
        else if (hasMetadata instanceof KafkaService service) {
            return isStatusFresh(service, i -> Optional.ofNullable(i.getStatus()).map(KafkaServiceStatus::getObservedGeneration).orElse(null));
        }
        else if (hasMetadata instanceof VirtualKafkaCluster cluster) {
            return isStatusFresh(cluster, c -> Optional.ofNullable(c.getStatus()).map(VirtualKafkaClusterStatus::getObservedGeneration).orElse(null));
        }
        else if (hasMetadata instanceof KafkaProxyIngress ingress) {
            return isStatusFresh(ingress, i -> Optional.ofNullable(i.getStatus()).map(KafkaProxyIngressStatus::getObservedGeneration).orElse(null));
        }
        else if (hasMetadata instanceof KafkaProxy kafkaProxy) {
            return isStatusFresh(kafkaProxy, i -> Optional.ofNullable(i.getStatus()).map(KafkaProxyStatus::getObservedGeneration).orElse(null));
        }
        else {
            throw new IllegalArgumentException("Unknown resource type: " + hasMetadata.getClass().getName());
        }
    }

    /**
     * Checks that the status contains a fresh ResolvedRefs=false condition. Fresh means that the
     * observedGeneration of the condition is equal to the metadata.generation of the resource.
     * @param hasMetadata hasMetadata
     * @throws IllegalStateException if hasMetadata is not a CustomResource type owned by the Kroxylicious Operator which uses ResolvedRefs Conditions
     * @return true if hasMetadata status contains a fresh ResolvedRefs=false condition
     */
    public static boolean hasFreshResolvedRefsFalseCondition(HasMetadata hasMetadata) {
        Objects.requireNonNull(hasMetadata);
        List<Condition> conditions;
        if (hasMetadata instanceof KafkaProtocolFilter filter) {
            conditions = Optional.ofNullable(filter.getStatus()).map(KafkaProtocolFilterStatus::getConditions).orElse(List.of());
        }
        else if (hasMetadata instanceof KafkaService service) {
            conditions = Optional.ofNullable(service.getStatus()).map(KafkaServiceStatus::getConditions).orElse(List.of());
        }
        else if (hasMetadata instanceof VirtualKafkaCluster cluster) {
            conditions = Optional.ofNullable(cluster.getStatus()).map(VirtualKafkaClusterStatus::getConditions).orElse(List.of());
        }
        else if (hasMetadata instanceof KafkaProxyIngress ingress) {
            conditions = Optional.ofNullable(ingress.getStatus()).map(KafkaProxyIngressStatus::getConditions).orElse(List.of());
        }
        else {
            throw new IllegalArgumentException("Resource kind '" + HasMetadata.getKind(hasMetadata.getClass()) + "' does not use ResolveRefs conditions");
        }
        return conditions.stream()
                .filter(condition -> condition.getObservedGeneration().equals(hasMetadata.getMetadata().getGeneration()))
                .anyMatch(Condition::isResolvedRefsFalse);
    }

    public static Predicate<HasMetadata> isStatusFresh() {
        return ResourcesUtil::isStatusFresh;
    }

    public static Predicate<HasMetadata> isStatusStale() {
        return isStatusFresh().negate();
    }

    public static Predicate<HasMetadata> hasFreshResolvedRefsFalseCondition() {
        return ResourcesUtil::hasFreshResolvedRefsFalseCondition;
    }

    public static Predicate<HasMetadata> hasKind(String kind) {
        return hasMetadata -> HasMetadata.getKind(hasMetadata.getClass()).equals(kind);
    }

    @SuppressWarnings("java:S4276") // ToLongFunction is not appropriate, since observedGeneration may be null
    private static <T extends HasMetadata> boolean isStatusFresh(T resource, Function<T, Long> observedGenerationFunc) {
        Long observedGeneration = observedGenerationFunc.apply(resource);
        Long generation = resource.getMetadata().getGeneration();
        return Objects.equals(generation, observedGeneration);
    }

    /**
     * Checks the validity of the given {@link CertificateRef} which appears in the {@code resource}.
     * Specifically, this checks if the reference refers to a Kubernetes Secret, if the Secret is
     * of the right type and if the Secret actually exists. If any of those conditions are false, a
     * condition is added to the resource and the modified resource returned. If the reference is
     * valid, null is returned.
     *
     * @param resource resource
     * @param context context
     * @param secretEventSourceName event source name used to resolve the secret
     * @param certRef certificate reference
     * @param path path to the certificate reference within the resource
     * @param statusFactory used to generate the condition.
     * @return modified resource if the certificate references is invalid, or null otherwise.
     * @param <T> custom resource type
     */
    public static <T extends CustomResource<?, ?>> ResourceCheckResult<T> checkCertRef(T resource,
                                                                                       CertificateRef certRef,
                                                                                       String path,
                                                                                       StatusFactory<T> statusFactory,
                                                                                       Context<T> context,
                                                                                       String secretEventSourceName) {
        if (isSecret(certRef)) {
            Optional<Secret> secretOpt = context.getSecondaryResource(Secret.class, secretEventSourceName);
            if (secretOpt.isEmpty()) {
                return new ResourceCheckResult<>(statusFactory.newFalseConditionStatusPatch(resource, ResolvedRefs,
                        Condition.REASON_REFS_NOT_FOUND,
                        path + ": referenced resource not found"), List.of());
            }
            else {
                Secret secret = secretOpt.get();
                if (!"kubernetes.io/tls".equals(secret.getType())) {
                    return new ResourceCheckResult<>(statusFactory.newFalseConditionStatusPatch(resource, ResolvedRefs,
                            Condition.REASON_INVALID_REFERENCED_RESOURCE,
                            path + ": referenced secret should have 'type: kubernetes.io/tls'"), List.of());
                }
                else {
                    return new ResourceCheckResult<>(null, List.of(secret));
                }
            }
        }
        else {
            return new ResourceCheckResult<>(statusFactory.newFalseConditionStatusPatch(resource, ResolvedRefs,
                    Condition.REASON_REF_GROUP_KIND_NOT_SUPPORTED,
                    path + ": supports referents: secrets"), List.of());
        }
    }

    /**
     * Checks the validity of the given {@link TrustAnchorRef} which appears in the {@code resource}.
     * Specifically, this checks if the reference refers to a Kubernetes ConfigMap, and if the ConfigMap
     * actually exists. It validates that the ConfigMap has a data item with key value {@code key} with
     * a value that is the key name of a second data item, containing the trust material.  The key
     * name of the trust material must end with .pem, .p12 or .jks.  This is used to determine the
     * trust store's type. If any of those conditions are false, a condition is added to the resource
     * and the modified resource returned. If the reference is valid, null is returned.
     *
     * @param resource resource
     * @param context context
     * @param eventSourceName event source name used to resolve the secret
     * @param trustAnchorRef certificate reference
     * @param path path to the certificate reference within the resource
     * @param statusFactory used to generate the condition.
     * @return modified resource if the certificate references is invalid, or null otherwise.
     *
     * @param <T> custom resource type
     */
    public static <T extends CustomResource<?, ?>> ResourceCheckResult<T> checkTrustAnchorRef(T resource,
                                                                                              Context<T> context,
                                                                                              String eventSourceName,
                                                                                              TrustAnchorRef trustAnchorRef,
                                                                                              String path,
                                                                                              StatusFactory<T> statusFactory) {
        if (isConfigMap(trustAnchorRef.getRef())) {
            Optional<ConfigMap> configMapOpt = context.getSecondaryResource(ConfigMap.class, eventSourceName);
            if (configMapOpt.isEmpty()) {
                return new ResourceCheckResult<>(statusFactory.newFalseConditionStatusPatch(resource, ResolvedRefs,
                        Condition.REASON_REFS_NOT_FOUND,
                        path + ": referenced resource not found"), List.of());
            }
            else {
                String key = trustAnchorRef.getKey();
                if (key == null) {
                    return new ResourceCheckResult<>(statusFactory.newFalseConditionStatusPatch(resource, ResolvedRefs,
                            Condition.REASON_INVALID,
                            path + " must specify 'key'"), List.of());
                }
                if (!key.endsWith(".pem")
                        && !key.endsWith(".p12")
                        && !key.endsWith(".jks")) {
                    return new ResourceCheckResult<>(statusFactory.newFalseConditionStatusPatch(resource, ResolvedRefs,
                            Condition.REASON_INVALID,
                            path + ".key should end with .pem, .p12 or .jks"), List.of());
                }
                else {
                    ConfigMap configMap = configMapOpt.get();
                    if (!configMap.getData().containsKey(trustAnchorRef.getKey())) {
                        return new ResourceCheckResult<>(statusFactory.newFalseConditionStatusPatch(resource, ResolvedRefs,
                                Condition.REASON_INVALID_REFERENCED_RESOURCE,
                                path + ": referenced resource does not contain key " + trustAnchorRef.getKey()), List.of());
                    }
                    else {
                        return new ResourceCheckResult<>(null, List.of(configMap));
                    }
                }
            }
        }
        else {
            return new ResourceCheckResult<>(statusFactory.newFalseConditionStatusPatch(resource, ResolvedRefs,
                    Condition.REASON_REF_GROUP_KIND_NOT_SUPPORTED,
                    path + " supports referents: configmaps"), List.of());
        }
    }
}
