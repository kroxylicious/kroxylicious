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

import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;

/**
 * The result of a deep resolution of the dependencies of a single VirtualKafkaCluster. Describes
 * whether there were any resolution problems like dangling references, or referenced resources
 * that have a ResolvedRefs: False condition.
 *
 * @param cluster the cluster resource being recursively resolved
 * @param proxyResolutionResult the resolution result for the KafkaProxy resource this cluster references
 * @param filterResolutionResults resolution results for all filters referenced by the cluster, in the same order they are declared in the VirtualKafkaCluster spec
 * @param serviceResolutionResult the resolution result for service referenced by the cluster
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
     *     <li>All references can be retrieved from kubernetes (we have no dangling refs)</li>
     *     <li>No referent has a ResolvedRefs=False condition, which declares that it has unresolved dependencies</li>
     * </ol>
     * @return true if fully resolved
     */
    public boolean allReferentsFullyResolved() {
        return !anyReferentResolvedRefsConditionsFalse() && !anyDanglingReferences();
    }

    /**
     * Find all dangling references (recursive) from this VirtualKafkaCluster
     * @return a stream of all dangling references
     */
    public Stream<ClusterResolutionResult.DanglingReference> findDanglingReferences() {
        return allReferentResolutionResults().filter(ResolutionResult::dangling)
                .map(resolutionResult1 -> new ClusterResolutionResult.DanglingReference(resolutionResult1.referrer(), resolutionResult1.reference()));
    }

    /**
     * Find all dangling references (recursive) from the specified referrer to the specified kind
     * @param fromReferrer a LocalRef identifying the Referrer
     * @param toKind a Referent kind
     * @return a stream of all dangling references from the specified LocalRef to the specified kind
     */
    public Stream<ClusterResolutionResult.DanglingReference> findDanglingReferences(LocalRef<?> fromReferrer, String toKind) {
        Objects.requireNonNull(fromReferrer);
        Objects.requireNonNull(toKind);
        return findDanglingReferences().filter(r -> r.referrer().equals(fromReferrer) && toKind.equals(r.absentRef().getKind()));
    }

    /**
     * Find all dangling references (recursive) from the specified fromKind to the specified toKind
     * @param fromKind a Referrer kind
     * @param toKind a Referent kind
     * @return a stream of all dangling references from the specified fromKind to the specified toKind
     */
    public Stream<ClusterResolutionResult.DanglingReference> findDanglingReferences(String fromKind, String toKind) {
        Objects.requireNonNull(fromKind);
        Objects.requireNonNull(toKind);
        return findDanglingReferences().filter(r -> fromKind.equals(r.referrer().getKind()) && toKind.equals(r.absentRef().getKind()));
    }

    /**
     * Find all referents with stale status (status.observedGeneration != metadata.generation)
     * @return a stream of LocalRefs to referents with stale status
     */
    // using wildcards intentionally
    @SuppressWarnings("java:S1452")
    public Stream<LocalRef<?>> findReferentsWithStaleStatus() {
        return referentsMatching(Referent::isStale);
    }

    /**
     * Find all referents of a specified kind with a ResolvedRefs=False condition
     * @return a stream of LocalRefs to referents with a ResolvedRefs=False condition
     */
    // using wildcards intentionally
    @SuppressWarnings("java:S1452")
    public Stream<LocalRef<?>> findReferentsWithResolvedRefsFalseCondition(String referentKind) {
        return findReferentsWithResolvedRefsFalseCondition().filter(r -> referentKind.equals(r.getKind()));
    }

    /**
     * Find all referents with a ResolvedRefs=False condition
     * @return a stream of LocalRefs to referents with a ResolvedRefs=False condition
     */
    // using wildcards intentionally
    @SuppressWarnings("java:S1452")
    public Stream<LocalRef<?>> findReferentsWithResolvedRefsFalseCondition() {
        return referentsMatching(Referent::hasResolvedRefsFalseCondition);
    }

    /**
     * @return true iff all referents and the cluster have fresh status (status.observedGeneration == metadata.generation)
     */
    public boolean allReferentsHaveFreshStatus() {
        return findReferentsWithStaleStatus().findAny().isEmpty();
    }

    /**
     * @param fromReferrer fromReferrer
     * @return true iff any references could not be resolved from a Referrer
     */
    public boolean anyDanglingRefsFrom(LocalRef<?> fromReferrer) {
        return findDanglingReferences().anyMatch(u -> u.referrer().equals(fromReferrer));
    }

    /**
     * @return true iff any references could not be resolved
     */
    public boolean anyDanglingReferences() {
        return findDanglingReferences().findAny().isPresent();
    }

    /**
     * @return true iff any Referent of the VirtualKafkaCluster has a ResolvedRefs=False condition
     */
    public boolean anyReferentResolvedRefsConditionsFalse() {
        return findReferentsWithResolvedRefsFalseCondition().findAny().isPresent();
    }

    /**
     * @return true iff this VirtualKafkaCluster is stale (status.observedGeneration == metadata.generation)
     */
    public boolean isClusterStale() {
        return Referent.from(cluster).isStale();
    }

    /**
     * @return true iff this VirtualKafkaCluster has a ResolvedRefs=False condition
     */
    public boolean isClusterResolvedRefsFalse() {
        return Referent.from(cluster).hasResolvedRefsFalseCondition();
    }

    private Stream<LocalRef<?>> referentsMatching(Predicate<Referent<?>> predicate) {
        return allReferentResolutionResults().filter(i -> (i.referent() != null) && predicate.test(i.referent()))
                .map(ResolutionResult::reference);
    }

    private Stream<ResolutionResult<?>> allReferentResolutionResults() {
        Stream<ResolutionResult<?>> proxyResults = Stream.of(proxyResolutionResult);
        Stream<ResolutionResult<?>> filterResults = filterResolutionResults.stream().map(Function.identity());
        Stream<ResolutionResult<?>> serviceResults = Stream.of(serviceResolutionResult);
        Stream<ResolutionResult<?>> ingressResults = ingressResolutionResults.stream()
                .flatMap(i -> Stream.concat(Stream.of(i.ingressResolutionResult()), Optional.ofNullable(i.proxyResolutionResult()).stream()));
        return Stream.of(proxyResults, filterResults, serviceResults, ingressResults).flatMap(Function.identity());
    }

    /**
     * Describes the case where an entity A references an entity B, but we can not find B.
     * @param referrer the Referrer
     * @param absentRef the ref that could not be found
     */
    public record DanglingReference(LocalRef<?> referrer, LocalRef<?> absentRef) {}
}
