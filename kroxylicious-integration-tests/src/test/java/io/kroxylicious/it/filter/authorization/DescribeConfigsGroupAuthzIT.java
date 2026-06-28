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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsRequestData.DescribeConfigsResource;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeConfigsResponseData.DescribeConfigsResult;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
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
import static org.assertj.core.api.Assertions.assertThat;

class DescribeConfigsGroupAuthzIT extends AuthzIT {

    private static final String ALICE_GROUP_NAME = "alice-group";
    private static final String BOB_GROUP_NAME = "bob-group";

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
                allow User with name = "bob" to DESCRIBE_CONFIGS Group with name = "%s";
                otherwise deny;
                """.formatted(ALICE_GROUP_NAME, BOB_GROUP_NAME));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, ALICE_GROUP_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.GROUP, BOB_GROUP_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DESCRIBE_CONFIGS, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = ClusterPrepUtils.createTopicsAndAcls(kafkaClusterWithAuthzAdmin, List.of(), aclBindings);
        this.topicIdsInProxiedCluster = ClusterPrepUtils.createTopicsAndAcls(kafkaClusterNoAuthzAdmin, List.of(), List.of());
        setConfigOnGroups(kafkaClusterWithAuthzAdmin);
        setConfigOnGroups(kafkaClusterNoAuthzAdmin);
    }

    private static void setConfigOnGroups(Admin admin) {
        try {
            admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.GROUP, ALICE_GROUP_NAME),
                    List.of(new AlterConfigOp(new ConfigEntry("consumer.session.timeout.ms", "59000"), AlterConfigOp.OpType.SET)),
                    new ConfigResource(ConfigResource.Type.GROUP, BOB_GROUP_NAME),
                    List.of(new AlterConfigOp(new ConfigEntry("consumer.session.timeout.ms", "59000"), AlterConfigOp.OpType.SET)))).all().get(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tidyClusters() {
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, List.of(), aclBindings);
        ClusterPrepUtils.deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, List.of(), List.of());
    }

    class DescribeConfigsEquivalence extends Equivalence<DescribeConfigsRequestData, DescribeConfigsResponseData> {

        private final DescribeConfigsRequestData requestData;

        DescribeConfigsEquivalence(short apiVersion, DescribeConfigsRequestData requestData) {
            super(apiVersion);
            this.requestData = requestData;
        }

        @Override
        public ApiKeys apiKey() {
            return ApiKeys.DESCRIBE_CONFIGS;
        }

        @Override
        public Map<String, String> passwords() {
            return PASSWORDS;
        }

        @Override
        public DescribeConfigsRequestData requestData(String user, BaseClusterFixture clusterFixture) {
            return requestData;
        }

        @Override
        public void assertUnproxiedResponses(Map<String, DescribeConfigsResponseData> unproxiedResponsesByUser) {
            assertUserHasResults(unproxiedResponsesByUser, ALICE, Map.of(ALICE_GROUP_NAME, Errors.NONE, BOB_GROUP_NAME, Errors.GROUP_AUTHORIZATION_FAILED));
            assertUserHasResults(unproxiedResponsesByUser, BOB, Map.of(ALICE_GROUP_NAME, Errors.GROUP_AUTHORIZATION_FAILED, BOB_GROUP_NAME, Errors.NONE));
            assertUserHasResults(unproxiedResponsesByUser, EVE,
                    Map.of(ALICE_GROUP_NAME, Errors.GROUP_AUTHORIZATION_FAILED, BOB_GROUP_NAME, Errors.GROUP_AUTHORIZATION_FAILED));
        }

        private static void assertUserHasResults(Map<String, DescribeConfigsResponseData> unproxiedResponsesByUser, String user, Map<String, Errors> expected) {
            List<DescribeConfigsResult> results = unproxiedResponsesByUser.get(user).results();
            assertThat(results).hasSize(expected.size());
            Map<String, Errors> resourceNameToError = results.stream().collect(Collectors.toMap(DescribeConfigsResult::resourceName, r -> Errors.forCode(r.errorCode())));
            assertThat(resourceNameToError).containsExactlyInAnyOrderEntriesOf(expected);
        }

        @Override
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonNodes) {
            return prettyJsonString(jsonNodes);
        }

    }

    List<Arguments> shouldEnforceAccessToTopics() {
        DescribeConfigsRequestData specificGroups = new DescribeConfigsRequestData();
        List.of(ALICE_GROUP_NAME, BOB_GROUP_NAME).forEach(group -> specificGroups.resources().add(groupResource(group)));
        Stream<Arguments> supportedVersions = IntStream.rangeClosed(AuthorizationFilter.minSupportedApiVersion(ApiKeys.DESCRIBE_CONFIGS),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.DESCRIBE_CONFIGS))
                .boxed().flatMap(apiVersion -> Stream.of(
                        Arguments.argumentSet("api version " + apiVersion + " specific topics request",
                                new DescribeConfigsEquivalence((short) (int) apiVersion, specificGroups))));
        Stream<Arguments> unsupportedVersions = IntStream
                .rangeClosed(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion(true))
                .filter(version -> !AuthorizationFilter.isApiVersionSupported(ApiKeys.DESCRIBE_CONFIGS, (short) version))
                .mapToObj(
                        apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion,
                                new UnsupportedApiVersion<>(ApiKeys.DESCRIBE_CONFIGS, (short) apiVersion)));
        return concat(supportedVersions, unsupportedVersions).toList();
    }

    private static DescribeConfigsResource groupResource(String group) {
        var resource = new DescribeConfigsResource();
        resource.setResourceName(group);
        resource.setResourceType(ConfigResource.Type.GROUP.id());
        return resource;
    }

    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(VersionSpecificVerification<DescribeTopicPartitionsRequestData, DescribeTopicPartitionsResponseData> test) {
        try (var referenceCluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster);
                var proxiedCluster = new ProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster, rulesFile)) {
            test.verifyBehaviour(referenceCluster, proxiedCluster);
        }
    }

}
