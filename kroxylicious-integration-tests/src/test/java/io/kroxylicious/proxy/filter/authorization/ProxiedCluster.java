/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.SaslConfigs;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
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

        ConfigurationBuilder builder = AuthzIT.proxyConfig(cluster, AuthzIT.PASSWORDS, rulesFile);
        builder.build();
        this.tester = kroxyliciousTester(builder);
    }

    @Override
    public String name() {
        return "proxied";
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

    @Override
    public Map<String, Object> getKafkaClientConfiguration(String username, String password) {
        Map<String, Object> clientConfiguration = tester.clientConfiguration();
        clientConfiguration.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        clientConfiguration.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        clientConfiguration.put(SaslConfigs.SASL_JAAS_CONFIG, """
                org.apache.kafka.common.security.plain.PlainLoginModule required
                    username="%s"
                    password="%s";""".formatted(username, password));
        return clientConfiguration;
    }
}
