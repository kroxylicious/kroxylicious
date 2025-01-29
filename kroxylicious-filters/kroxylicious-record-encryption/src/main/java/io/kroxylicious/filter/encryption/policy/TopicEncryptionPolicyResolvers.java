/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.policy;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class TopicEncryptionPolicyResolvers {

    private static final TopicEncryptionPolicyResolver LEGACY_RESOLVER = allTopics(TopicEncryptionPolicy.LEGACY);
    private static final TopicEncryptionPolicyResolver REQUIRE_ENCRYPTION = allTopics(TopicEncryptionPolicy.REQUIRE_ENCRYPTION);

    public static TopicEncryptionPolicyResolver legacy() {
        return LEGACY_RESOLVER;
    }

    public static TopicEncryptionPolicyResolver requireEncryption() {
        return REQUIRE_ENCRYPTION;
    }

    private static TopicEncryptionPolicyResolver allTopics(TopicEncryptionPolicy policy) {
        Objects.requireNonNull(policy);
        return topics -> {
            Objects.requireNonNull(topics);
            return CompletableFuture.completedFuture(
                    topics.stream().collect(Collectors.toMap(t -> t, t -> policy)));
        };
    }
}
