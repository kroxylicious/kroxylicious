/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.policy;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A resolver for obtaining TopicEncryptionPolicies for a set of topic names. This interface is
 * asynchronous to allow for future implementations to retrieve remote topic metadata to inform
 * the encryption policy.
 */
public interface TopicEncryptionPolicyResolver {

    /**
     * Resolves a TopicPolicy for every input topicName
     * @param topicNames topic names, non-null
     * @return a completionstage for a map with a non-null value for every name in {@code topicNames}
     * @throws NullPointerException if topicNames is null
     */
    CompletionStage<Map<String, TopicEncryptionPolicy>> apply(@NonNull Set<String> topicNames);

}
