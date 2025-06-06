/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.List;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.HasMetadata;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Ingresses;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The result of resolving a VKC's {@code spec.ingresses.ingressRef} and {@code spec.ingresses.proxyRef}
 *
 * @param ingressResolutionResult the resolved KafkaProxyIngress result
 * @param proxyResolutionResult the resolved KafkaProxy result
 * @param ingress the virtual kafka cluster ingress that was being resolved
 * @param secretResolutionResults
 */
public record IngressResolutionResult(ResolutionResult<KafkaProxyIngress> ingressResolutionResult,
                                      @Nullable ResolutionResult<KafkaProxy> proxyResolutionResult,
                                      Ingresses ingress, List<ResolutionResult<? extends HasMetadata>> secretResolutionResults) {

    /**
     *
     * @return A stream of all the different things with metadata referenced by the ingress node.
     */
    public Stream<ResolutionResult<? extends HasMetadata>> allResolutionResults() {
        Stream.Builder<ResolutionResult<? extends HasMetadata>> builder = Stream.builder();
        builder.add(ingressResolutionResult);
        if (proxyResolutionResult != null) {
            builder.add(proxyResolutionResult);
        }
        secretResolutionResults.forEach(builder::add);
        return builder.build();
    }
}
