/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.provider.opa;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class OpaData {
    @JsonProperty("topics")
    @Nullable
    Map<String, TopicData> topics;

    @JsonProperty("prefixes")
    @Nullable
    List<PrefixData> prefixes;

    @JsonProperty("patterns")
    @Nullable
    List<PatternData> patterns;

    @JsonProperty("anyResourceUsers")
    @Nullable
    String[] anyResourceUsers;

    @JsonProperty("anyResourceSubscribers")
    @Nullable
    String[] anyResourceSubscribers;

    public OpaData() {
    }

    public OpaData(Map<String, TopicData> topics) {
        this.topics = topics;
    }

    public Map<String, TopicData> topics() {
        return topics;
    }

    public List<PrefixData> prefixes() {
        return prefixes;
    }

    public List<PatternData> patterns() {
        return patterns;
    }

    public String[] anyResourceUsers() {
        return anyResourceUsers;
    }

    public String[] anyResourceSubscribers() {
        return anyResourceSubscribers;
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

    public static final class PrefixData {
        @JsonProperty("prefix")
        @Nullable
        String prefix;

        @JsonProperty("owner")
        @Nullable
        String owner;

        @JsonProperty("subscribers")
        @Nullable
        String[] subscribers;

        public PrefixData() {
        }

        public String prefix() {
            return prefix;
        }

        public String owner() {
            return owner;
        }

        public String[] subscribers() {
            return subscribers;
        }
    }

    public static final class PatternData {
        @JsonProperty("pattern")
        @Nullable
        String pattern;

        @JsonProperty("owner")
        @Nullable
        String owner;

        @JsonProperty("subscribers")
        @Nullable
        String[] subscribers;

        public PatternData() {
        }

        public String pattern() {
            return pattern;
        }

        public String owner() {
            return owner;
        }

        public String[] subscribers() {
            return subscribers;
        }
    }
}
