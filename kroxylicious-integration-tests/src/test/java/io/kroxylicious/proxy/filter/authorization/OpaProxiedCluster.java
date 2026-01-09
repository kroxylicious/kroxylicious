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

import io.kroxylicious.authorizer.provider.opa.OpaAuthorizerService;
import io.kroxylicious.filter.authorization.Authorization;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.proxy.testplugins.SaslPlainTermination;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;

/**
 * A proxied cluster where authorization is being done by the OPA authorizer.
 */
public final class OpaProxiedCluster implements BaseClusterFixture {
    private final Map<String, Uuid> topicIds;
    private final Path opaPolicyFile;
    private final Path opaDataFile;
    private final KroxyliciousTester tester;
    private final KafkaCluster backingCluster;

    OpaProxiedCluster(KafkaCluster cluster,
                      Map<String, Uuid> topicIds,
                      Path opaPolicyFile,
                      Path opaDataFile) {
        this.backingCluster = cluster;
        this.topicIds = topicIds;
        this.opaPolicyFile = opaPolicyFile;
        this.opaDataFile = opaDataFile;

        ConfigurationBuilder builder = proxyConfigOpa(cluster, AuthzIT.PASSWORDS, opaPolicyFile, opaDataFile);
        builder.build();
        this.tester = kroxyliciousTester(builder);
    }

    @Override
    public String name() {
        return "opa-proxied";
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

    /**
     * Helper method to configure proxy with OPA authorizer
     */
    static ConfigurationBuilder proxyConfigOpa(KafkaCluster proxiedCluster,
                                               Map<String, String> passwords,
                                               Path opaPolicyFile,
                                               Path opaDataFile) {
        NamedFilterDefinition saslTermination = new NamedFilterDefinitionBuilder(
                "authn",
                SaslPlainTermination.class.getName())
                .withConfig("userNameToPassword", passwords)
                .build();

        // Configure OPA authorizer
        Map<String, Object> opaConfig = Map.of(
                "opaFile", opaPolicyFile.toFile().getAbsolutePath(),
                "dataFile", opaDataFile.toFile().getAbsolutePath());

        NamedFilterDefinition authorization = new NamedFilterDefinitionBuilder(
                "authz",
                Authorization.class.getName())
                .withConfig("authorizer", OpaAuthorizerService.class.getName(),
                        "authorizerConfig", opaConfig)
                .build();

        return proxy(proxiedCluster)
                .addToFilterDefinitions(saslTermination, authorization)
                .addToDefaultFilters(saslTermination.name(), authorization.name())
                .editMatchingVirtualCluster(x -> true)
                .endVirtualCluster();
    }
}
