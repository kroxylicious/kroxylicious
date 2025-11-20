/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import io.kroxylicious.filter.authorization.AuthorizationFilter;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;

import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AuthzFailsClosedIT extends AuthzIT {

    @Test
    void shouldFailClosedWhenUserExpectsAuthzOverUnsupportedResourceTypes() throws IOException {
        var rulesFile = Files.createTempFile(AuthzIT.class.getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.proxy.filter.authorization import UnsupportedResourceType;
                allow User with name * to * UnsupportedResourceType with name *;
                otherwise deny;
                """);

        ConfigurationBuilder builder = proxyConfig(kafkaClusterNoAuthz, Map.of(), rulesFile);
        assertThatThrownBy(() -> {
            try (var ignored = kroxyliciousTester(builder)) {
                return; // stupid conflicting code warnings
            }
        })
                .isInstanceOf(PluginConfigurationException.class).hasMessageMatching(
                        "Exception initializing filter factory authz with config AuthorizationConfig.*?: "
                                + "io.kroxylicious.authorizer.provider.acl.AclAuthorizerService "
                                + "specifies access controls for resource types which cannot be enforced by this filter. "
                                + "The unsupported types are: io.kroxylicious.proxy.filter.authorization.UnsupportedResourceType.");

    }

    @TestFactory
    Stream<DynamicNode> shouldFailClosedForUnsupportedApiKeysAndVersions() throws IOException {
        // TODO we should check that the ApiVersions response does not include these keys
        var rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TopicResource as Topic;
                allow User with name * to * Topic with name *;
                otherwise deny;
                """);

        var proxiedCluster = new ProxiedCluster(
                kafkaClusterNoAuthz,
                topicIdsInProxiedCluster,
                rulesFile);

        Stream<DynamicNode> dynamicNodeStream = Arrays.stream(ApiKeys.values())
                .map(apiKey -> {
                    Stream<Short> apiVersions;

                    apiVersions = apiKey.allVersions().stream().filter(apiVersion -> !AuthorizationFilter.isApiVersionSupported(apiKey, apiVersion));

                    return DynamicContainer.dynamicContainer(apiKey.toString(), apiVersions.map(apiVersion -> {
                        return DynamicTest.dynamicTest("v" + apiVersion, () -> {

                            var unsupportedVersion = new UnsupportedApiVersion<>(apiKey, apiVersion);
                            verifyUnsupportedVersion(proxiedCluster, unsupportedVersion);
                        });
                    }));
                });
        return dynamicNodeStream.onClose(proxiedCluster::close);
    }

}
