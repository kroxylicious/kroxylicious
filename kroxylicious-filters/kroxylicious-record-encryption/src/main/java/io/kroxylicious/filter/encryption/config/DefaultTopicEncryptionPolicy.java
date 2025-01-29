/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

/**
 * In no specific TopicEncryptionPolicy can be determined for a topic, this defines the default policy to be applied.
 * (Note that currently there is no mechanism to specify a TopicEncryptionPolicy per topic)
 */
public enum DefaultTopicEncryptionPolicy {
    /**
     * All data must be encrypted to be forwarded towards the target cluster
     */
    REQUIRE_ENCRYPTION
}
