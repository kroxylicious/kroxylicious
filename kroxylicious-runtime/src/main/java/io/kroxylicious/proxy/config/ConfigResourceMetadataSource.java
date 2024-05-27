/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.List;

import io.kroxylicious.proxy.internal.metadata.config.TopicLabelling;

public record ConfigResourceMetadataSource(
                                           List<TopicLabelling> topicLabellings) {

}
