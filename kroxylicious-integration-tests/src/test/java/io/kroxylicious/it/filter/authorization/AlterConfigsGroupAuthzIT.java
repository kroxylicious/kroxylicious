/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.AlterConfigsRequestData;
import org.apache.kafka.common.message.AlterConfigsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kroxylicious.filter.authorization.AuthorizationFilter;
import io.kroxylicious.testing.kafka.junit5ext.Name;

import static java.util.stream.Stream.concat;

class AlterConfigsGroupAuthzIT extends AuthzIT {

    private static final String ALICE_GROUP_NAME = "alice-group";
    private static final String BOB_GROUP_NAME = "bob-group";
    private static final List<String> ALL_GROUPS = List.of(ALICE_GROUP_NAME, BOB_GROUP_NAME);

    private Path rulesFile;

    private List<AclBinding> aclBindings;

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;
    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    @BeforeAll
    void beforeAll() throws IOException {
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import GroupResource as Group;
                allow User with name = "alice" to * Group with name = "%s";
                allow User with name = "bob" to ALTER_CONFIGS Group with name = "%s";
                otherwise deny;
                """.formatted(ALICE_GROUP_NAME, BOB_GROUP_NAME));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                // topic permissions
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, ALICE_GROUP_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, BOB_GROUP_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.ALTER_CONFIGS, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = ClusterPrepUtils.createTopicsAndAcls(kafkaClusterWithAuthzAdmin, List.of(), aclBindings);
        this.topicIdsInProxiedCluster = ClusterPrepUtils.createTopicsAndAcls(kafkaClusterNoAuthzAdmin, List.of(), List.of());
    }

    @AfterEach
    void tidyClusters() {
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, List.of(), aclBindings);
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, List.of(), List.of());
    }

    class AlterConfigsEquivalence extends Equivalence<AlterConfigsRequestData, AlterConfigsResponseData> {

        private final AlterConfigsRequestData requestData;

        AlterConfigsEquivalence(short apiVersion, AlterConfigsRequestData requestData) {
            super(apiVersion);
            this.requestData = requestData;
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.ALTER_CONFIGS;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public AlterConfigsRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            return requestData;
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonNodes) {
            sortArray(jsonNodes, "responses", "resourceName");
            return prettyJsonString(jsonNodes);
        }

    }

    List<Arguments> shouldEnforceAccessToGroups() {
        AlterConfigsRequestData alterConfigs = new AlterConfigsRequestData();
        ALL_GROUPS.forEach(group -> alterConfigs.resources().add(groupResource(group)));
        Stream<Arguments> supportedVersions = IntStream.rangeClosed(AuthorizationFilter.minSupportedApiVersion(ApiKeys.ALTER_CONFIGS),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.ALTER_CONFIGS))
                .boxed().flatMap(apiVersion -> Stream.of(
                        Arguments.argumentSet("api version " + apiVersion + " specific groups request",
                                new AlterConfigsEquivalence((short) (int) apiVersion, alterConfigs))));
        Stream<Arguments> unsupportedVersions = IntStream
                .rangeClosed(ApiKeys.ALTER_CONFIGS.oldestVersion(), ApiKeys.ALTER_CONFIGS.latestVersion(true))
                .filter(version -> !AuthorizationFilter.isApiVersionSupported(ApiKeys.ALTER_CONFIGS, (short) version))
                .mapToObj(
                        apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion,
                                new UnsupportedApiVersion<>(ApiKeys.ALTER_CONFIGS, (short) apiVersion)));
        return concat(supportedVersions, unsupportedVersions).toList();
    }

    private static AlterConfigsRequestData.AlterConfigsResource groupResource(String topicName) {
        var resource = new AlterConfigsRequestData.AlterConfigsResource();
        resource.setResourceName(topicName);
        resource.setResourceType(ConfigResource.Type.GROUP.id());
        AlterConfigsRequestData.AlterableConfig alterable = new AlterConfigsRequestData.AlterableConfig();
        alterable.setName("session.timeout.ms");
        alterable.setValue("20000");
        resource.configs().add(alterable);
        return resource;
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToGroups(VersionSpecificVerification<AlterConfigsRequestData, AlterConfigsResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

}
