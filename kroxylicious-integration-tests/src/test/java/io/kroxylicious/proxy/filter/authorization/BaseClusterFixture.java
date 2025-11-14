/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.Uuid;

import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.testing.kafka.api.KafkaCluster;

/**
 * Abstraction over some {@code KafkaCluster} used in a test.
 */
public interface BaseClusterFixture extends AutoCloseable {

    String name();

    /**
     * @return The bootstrap server for test clients to use
     */
    String clientBootstrap();

    /**
     * A mapping from topic name to id, for any topics needed by a test
     */
    Map<String, Uuid> topicIds();

    KafkaCluster backingCluster();

    /**
     * @return authenticated clients, ready for making requests, for each of the given usernames.
     */
    default Map<String, KafkaClient> authenticatedClients(Set<String> usernames) {
        return usernames.stream().collect(Collectors.toMap(Function.identity(),
                username -> {
                    var client = AuthzIT.client(clientBootstrap());
                    AuthzIT.authenticate(client, username, AuthzIT.PASSWORDS.get(username));
                    return client;
                }));
    }

    Map<String, Object> getKafkaClientConfiguration(String username, String password);

    /**
     * Close any closeable resources created by this fixture
     */
    @Override
    void close();
}
