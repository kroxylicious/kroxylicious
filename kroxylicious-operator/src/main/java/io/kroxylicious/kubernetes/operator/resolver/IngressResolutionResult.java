/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

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
                                      Ingresses ingress, java.util.List<ResolutionResult<io.fabric8.kubernetes.api.model.Secret>> secretResolutionResults) {}
