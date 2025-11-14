/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.util.Map;

import org.apache.kafka.common.Uuid;

import io.kroxylicious.testing.kafka.api.KafkaCluster;

/**
 * An unproxied references cluster for use with
 * {@link AuthzIT#verifyApiEqivalence(ReferenceCluster, ProxiedCluster, AuthzIT.Equivalence)}
 * the responses from which are assumed to be the canonical Kafka behaviour
 *  which the {@link io.kroxylicious.filter.authorization.AuthorizationFilter} is
 *  attempting to replicate.
 */
public final class ReferenceCluster implements BaseClusterFixture {

    private final KafkaCluster cluster;
    private final Map<String, Uuid> topicIds;

    ReferenceCluster(KafkaCluster cluster,
                     Map<String, Uuid> topicIds) {
        this.cluster = cluster;
        this.topicIds = topicIds;
    }

    @Override
    public String name() {
        return "reference";
    }

    @Override
    public String clientBootstrap() {
        return cluster.getBootstrapServers();
    }

    @Override
    public Map<String, Uuid> topicIds() {
        return topicIds;
    }

    @Override
    public KafkaCluster backingCluster() {
        return cluster;
    }

    @Override
    public Map<String, Object> getKafkaClientConfiguration(String username, String password) {
        return cluster.getKafkaClientConfiguration(username, password);
    }

    @Override
    public void close() {

    }
}
