/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.common.Uuid;

import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;

import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;

/**
 * A proxied cluster where authorization is being done by the AuthorizationFiler that's under test.
 */
public final class ProxiedCluster implements BaseClusterFixture {
    private final Map<String, Uuid> topicIds;
    private final Path rulesFile;
    private final KroxyliciousTester tester;
    private final KafkaCluster backingCluster;

    ProxiedCluster(KafkaCluster cluster,
                   Map<String, Uuid> topicIds,
                   Path rulesFile) {
        this.backingCluster = cluster;
        this.topicIds = topicIds;
        this.rulesFile = rulesFile;

        this.tester = kroxyliciousTester(AuthzIT.proxyConfig(cluster, AuthzIT.PASSWORDS, rulesFile));
    }

    @Override
    public String clientBootstrap() {
        return tester.getBootstrapAddress();
    }

    @Override
    public Map<String, Uuid> topicIds() {
        return topicIds;
    }

    @Override
    public KafkaCluster backingCluster() {
        return backingCluster;
    }

    public Path rulesFile() {
        return rulesFile;
    }

    public void close() {
        tester.close();
    }
}
