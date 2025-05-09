/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The result of dereferencing an Ingresses ingressRef and proxyRef
 * @param ingressResolutionResult the dereferenced KafkaProxyIngress result
 * @param proxyResolutionResult the dereferenced KafkaProxy result
 */
public record IngressResolutionResult(ResolutionResult<KafkaProxyIngress> ingressResolutionResult,
                                      @Nullable ResolutionResult<KafkaProxy> proxyResolutionResult) {}
