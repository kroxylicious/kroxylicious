/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

/**
 * We want internal filters to be able to target different types of messages:
 * <ul>
 *     <li>client messages being passed through the proxy</li>
 *     <li>specific framework initiated messages, which the framework has marked as cacheable</li>
 * </ul>
 */
public enum TargetMessageClass {
    /**
     * Target all messages
     */
    ALL,

    /**
     * Target only messages initiated by the framework that are marked as being cacheable at the
     * edge of the proxy/upstream. The intent is to install some filter at the edge that can cache
     * responses from the upstream, for instance to hold topic id to topic name mappings.
     */
    INTERNAL_EDGE_CACHEABLE
}
