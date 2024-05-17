/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.metadata;

import java.util.Collection;

import io.kroxylicious.proxy.metadata.selector.Selector;

/**
 * A request to list which of the topics identified by the given {@code topicNames} match any of the given {@code selectors}.
 * @param topicNames The topic names
 * @param selectors The selectors
 */
public record ListTopicsRequest(Collection<String> topicNames,
                                Collection<Selector> selectors)
        implements ResourceMetadataRequest<ListTopicsResponse> {}
