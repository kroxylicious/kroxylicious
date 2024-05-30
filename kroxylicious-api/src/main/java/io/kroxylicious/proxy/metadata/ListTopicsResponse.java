/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.metadata;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

import io.kroxylicious.proxy.metadata.selector.Selector;

/**
 * A response to a {@link ListTopicsRequest} detailing which of the topics in the request match which selector.
 */
public final class ListTopicsResponse implements ResourceMetadataResponse<ListTopicsRequest> {
    private final Map<Selector, Set<String>> selectedTopicNames;

    /**
     * @param selectedTopicNames
     */
    public ListTopicsResponse(Map<Selector, Set<String>> selectedTopicNames) {
        this.selectedTopicNames = selectedTopicNames;
    }

    public Set<String> topicsMatching(Selector selector) {
        return Collections.unmodifiableSet(selectedTopicNames.get(selector));
    }

    public void forEach(BiConsumer<Selector, Set<String>> consumer) {
        selectedTopicNames.forEach(((selector, strings) -> consumer.accept(selector, Collections.unmodifiableSet(strings))));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (ListTopicsResponse) obj;
        return Objects.equals(this.selectedTopicNames, that.selectedTopicNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(selectedTopicNames);
    }

    @Override
    public String toString() {
        return "ListTopicsResponse[" +
                "selectedTopicNames=" + selectedTopicNames + ']';
    }

}
