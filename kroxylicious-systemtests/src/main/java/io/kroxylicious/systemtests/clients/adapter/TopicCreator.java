/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.clients.adapter;

/**
 * Creates topics on behalf of an adapter whose underlying driver cannot.
 */
@FunctionalInterface
interface TopicCreator {

    void createTopic(String topic, String bootstrap, int partitions, int replicationFactor);
}
