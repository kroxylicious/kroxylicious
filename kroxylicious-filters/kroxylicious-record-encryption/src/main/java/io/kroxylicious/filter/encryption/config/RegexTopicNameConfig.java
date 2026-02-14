/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import java.util.List;
import java.util.Objects;

import edu.umd.cs.findbugs.annotations.Nullable;

public record RegexTopicNameConfig(List<Rule> rules) {

    public RegexTopicNameConfig {
        Objects.requireNonNull(rules);
        if (rules.isEmpty()) {
            throw new IllegalArgumentException("rules is empty");
        }
    }

    public enum Behaviour {
        ENCRYPT,
        PASSTHROUGH_UNENCRYPTED
    }

    public record Rule(Behaviour behaviour,
                       @Nullable String kekTemplate,
                       List<String> topicPatterns) {
        public Rule {
            Objects.requireNonNull(behaviour, "behaviour was null");
            if (behaviour == Behaviour.ENCRYPT) {
                Objects.requireNonNull(kekTemplate, "when behaviour is ENCRYPT, the 'kekTemplate' field is required");
            }
            Objects.requireNonNull(topicPatterns);
            if (topicPatterns.isEmpty()) {
                throw new IllegalArgumentException("topic patterns is empty");
            }
        }
    }
}
