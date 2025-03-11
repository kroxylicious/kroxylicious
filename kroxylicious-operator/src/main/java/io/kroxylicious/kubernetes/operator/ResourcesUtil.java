/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaClusterRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

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

    static Map<ResourceID, KafkaClusterRef> clusterRefs(Context<KafkaProxy> context) {
        return context.getSecondaryResources(KafkaClusterRef.class)
                .stream()
                .collect(Collectors.toMap(ResourceID::fromResource, Function.identity()));
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
}
