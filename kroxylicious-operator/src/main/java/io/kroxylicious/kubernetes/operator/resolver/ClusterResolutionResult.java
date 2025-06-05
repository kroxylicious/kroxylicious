/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.HasMetadata;

import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

/**
 * The result of a deep resolution of the transitive references of a single VirtualKafkaCluster. Describes
 * whether there were any resolution problems like dangling references, or Referents that have a ResolvedRefs=False
 * condition, or Referents that have a stale status.
 *
 * @param cluster the cluster resource that was resolved
 * @param proxyResolutionResult the resolution result for the KafkaProxy resource this cluster references
 * @param filterResolutionResults resolution results for all filters referenced by the cluster, in the same order they are declared in the VirtualKafkaCluster spec,
 *         note that the same Filter can be in this list multiple times
 * @param serviceResolutionResult the resolution result for the kafka service referenced by the cluster
 * @param ingressResolutionResults the resolution results for ingresses referenced by the cluster, in the same order they are declared in the VirtualKafkaCluster spec
 */
public record ClusterResolutionResult(VirtualKafkaCluster cluster,
                                      ResolutionResult<KafkaProxy> proxyResolutionResult,
                                      List<ResolutionResult<KafkaProtocolFilter>> filterResolutionResults,
                                      ResolutionResult<KafkaService> serviceResolutionResult,
                                      List<IngressResolutionResult> ingressResolutionResults) {

    /**
     * All referents are fully resolved iff
     * <ol>
     *     <li>All references can be retrieved from kubernetes (we have no dangling references)</li>
     *     <li>No referent has a ResolvedRefs=False condition, which declares that it has dangling references</li>
     * </ol>
     * @return true if fully resolved
     */
    public boolean allReferentsFullyResolved() {
        return allDanglingReferences().findAny().isEmpty() && allResolvedReferents()
                .noneMatch(ResourcesUtil::hasFreshResolvedRefsFalseCondition);
    }

    /**
     * @return a stream of all dangling references
     */
    public Stream<DanglingReference> allDanglingReferences() {
        return allResolutionResults().filter(ResolutionResult::dangling)
                .map(result -> new DanglingReference(result.referrer(), result.reference()));
    }

    /**
     * @return a stream of all resolved referents of this Cluster, note that KafkaProxy is not considered a Referent of VirtualKafkaCluster to prevent cycles
     */
    public Stream<HasMetadata> allResolvedReferents() {
        return allResolutionResults()
                .filter(resolutionResult -> !Objects.equals(resolutionResult.reference().getKind(), HasMetadata.getKind(KafkaProxy.class)))
                .flatMap(resolutionResult -> resolutionResult.maybeReferentResource().stream());
    }

    private Stream<ResolutionResult<? extends HasMetadata>> allResolutionResults() {
        Stream<ResolutionResult<? extends HasMetadata>> proxyResults = Stream.of(proxyResolutionResult);
        Stream<ResolutionResult<? extends HasMetadata>> filterResults = filterResolutionResults.stream().map(Function.identity());
        Stream<ResolutionResult<? extends HasMetadata>> serviceResults = Stream.of(serviceResolutionResult);
        Stream<ResolutionResult<? extends HasMetadata>> ingressResults = ingressResolutionResults.stream()
                .flatMap(i -> Stream.concat(Stream.of(i.ingressResolutionResult()), Optional.ofNullable(i.proxyResolutionResult()).stream()));
        return Stream.of(proxyResults, filterResults, serviceResults, ingressResults).flatMap(Function.identity());
    }

    /**
     * Describes the case where an entity A references an entity B, but we can not find B.
     * @param referrer the Referrer
     * @param absentRef the ref that could not be found
     */
    public record DanglingReference(LocalRef<?> referrer, LocalRef<?> absentRef) {
        public static Predicate<DanglingReference> hasReferrerKind(String kind) {
            return p -> Objects.equals(p.referrer.getKind(), kind);
        }

        public static Predicate<DanglingReference> hasReferentKind(String kind) {
            return p -> Objects.equals(p.absentRef.getKind(), kind);
        }

        public static Predicate<DanglingReference> hasReferrer(LocalRef<?> referrer) {
            return p -> p.referrer.equals(referrer);
        }
    }
}
