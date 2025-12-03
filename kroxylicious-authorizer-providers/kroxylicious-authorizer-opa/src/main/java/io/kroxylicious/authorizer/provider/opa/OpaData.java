/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.opa;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

public final class OpaData {
    @JsonProperty("topics")
    @Nullable
    Map<String, TopicData> topics;

    public OpaData() {
    }

    public OpaData(Map<String, TopicData> topics) {
        this.topics = topics;
    }

    public Map<String, TopicData> topics() {
        return topics;
    }

    public static final class TopicData {
        @JsonProperty("owner")
        @Nullable
        String owner;

        @JsonProperty("subscribers")
        @Nullable
        String[] subscribers;

        public TopicData() {
        }

        public TopicData(String owner, String[] subscribers) {
            this.owner = owner;
            this.subscribers = subscribers;
        }

        public String owner() {
            return owner;
        }

        public String[] subscribers() {
            return subscribers;
        }
    }
}
