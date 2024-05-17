/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.metadata;

import java.util.Map;
import java.util.Set;

import io.kroxylicious.proxy.metadata.selector.Selector;

/**
 * A response to a {@link ListTopicsRequest} detailing which of the topics in the request match which selector.
 * @param selectedTopicNames
 */
public record ListTopicsResponse(Map<Selector, Set<String>> selectedTopicNames) implements ResourceMetadataResponse<ListTopicsRequest> {

}
