/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.metadata;

import java.util.Map;
import java.util.Objects;

/**
 * A response to a {@link DescribeTopicLabelsRequest} detailing the labels of each of the topics.
 */
public final class DescribeTopicLabelsResponse implements ResourceMetadataResponse<DescribeTopicLabelsRequest> {
    private final Map<String, Map<String, String>> topicLabels;

    /**
     * @param topicLabels
     */
    public DescribeTopicLabelsResponse(Map<String, Map<String, String>> topicLabels) {
        this.topicLabels = topicLabels;
    }

    public Map<String, String> topicLabels(String topicName) {
        return topicLabels.get(topicName);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (DescribeTopicLabelsResponse) obj;
        return Objects.equals(this.topicLabels, that.topicLabels);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicLabels);
    }

    @Override
    public String toString() {
        return "DescribeTopicLabelsResponse[" +
                "topicLabels=" + topicLabels + ']';
    }

}
