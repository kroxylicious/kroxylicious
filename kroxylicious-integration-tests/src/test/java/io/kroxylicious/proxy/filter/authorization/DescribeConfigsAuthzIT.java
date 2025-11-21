/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsRequestData.DescribeConfigsResource;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
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

public class DescribeConfigsAuthzIT extends AuthzIT {

    private static final String ALICE_TOPIC_NAME = "alice-topic";
    private static final String BOB_TOPIC_NAME = "bob-topic";
    private static final String EVE_TOPIC_NAME = "eve-topic";
    private static final String EXISTING_TOPIC_NAME = "existing-topic";
    public static final List<String> ALL_TOPIC_NAMES_IN_TEST = List.of(
            ALICE_TOPIC_NAME,
            BOB_TOPIC_NAME,
            EVE_TOPIC_NAME,
            EXISTING_TOPIC_NAME);

    private Path rulesFile;

    private List<AclBinding> aclBindings;

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;
    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    @BeforeAll
    void beforeAll() throws IOException, ExecutionException, InterruptedException {
        rulesFile = Files.createTempFile(getClass().getName(), ".aclRules");
        Files.writeString(rulesFile, """
                from io.kroxylicious.filter.authorization import TopicResource as Topic;
                allow User with name = "alice" to * Topic with name = "%s";
                allow User with name = "bob" to DESCRIBE_CONFIGS Topic with name = "%s";
                otherwise deny;
                """.formatted(ALICE_TOPIC_NAME, BOB_TOPIC_NAME));
        /*
         * The correctness of this test is predicated on the equivalence of the Proxy ACLs (above) and the Kafka ACLs (below)
         * If you add a rule to one you'll need to add an equivalent rule to the other
         */
        aclBindings = List.of(
                // topic permissions
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, ALICE_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + ALICE, "*",
                                AclOperation.ALL, AclPermissionType.ALLOW)),
                new AclBinding(
                        new ResourcePattern(ResourceType.TOPIC, BOB_TOPIC_NAME, PatternType.LITERAL),
                        new AccessControlEntry("User:" + BOB, "*",
                                AclOperation.DESCRIBE_CONFIGS, AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        this.topicIdsInProxiedCluster = prepCluster(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, ALL_TOPIC_NAMES_IN_TEST, List.of());
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
        public String clobberResponse(BaseClusterFixture cluster, ObjectNode jsonNodes) {
            return prettyJsonString(jsonNodes);
        }

        @Override
        public void assertVisibleSideEffects(BaseClusterFixture cluster) {
        }

        @Override
        public void assertUnproxiedResponses(Map<String, DescribeConfigsResponseData> unproxiedResponsesByUser) {

        }

    }

    List<Arguments> shouldEnforceAccessToTopics() {
        DescribeConfigsRequestData specificTopics = new DescribeConfigsRequestData();
        ALL_TOPIC_NAMES_IN_TEST.forEach(topicName -> {
            specificTopics.resources().add(topicResource(topicName));
        });
        Stream<Arguments> supportedVersions = IntStream.rangeClosed(AuthorizationFilter.minSupportedApiVersion(ApiKeys.DESCRIBE_CONFIGS),
                AuthorizationFilter.maxSupportedApiVersion(ApiKeys.DESCRIBE_CONFIGS))
                .boxed().flatMap(apiVersion -> Stream.of(
                        Arguments.argumentSet("api version " + apiVersion + " specific topics request",
                                new DescribeConfigsEquivalence((short) (int) apiVersion, specificTopics))));
        Stream<Arguments> unsupportedVersions = IntStream
                .rangeClosed(ApiKeys.DESCRIBE_CONFIGS.oldestVersion(), ApiKeys.DESCRIBE_CONFIGS.latestVersion(true))
                .filter(version -> !AuthorizationFilter.isApiVersionSupported(ApiKeys.DESCRIBE_CONFIGS, (short) version))
                .mapToObj(
                        apiVersion -> Arguments.argumentSet("unsupported version " + apiVersion,
                                new UnsupportedApiVersion<>(ApiKeys.DESCRIBE_CONFIGS, (short) apiVersion)));
        return concat(supportedVersions, unsupportedVersions).toList();
    }

    private static DescribeConfigsResource topicResource(String topicName) {
        var resource = new DescribeConfigsResource();
        resource.setResourceName(topicName);
        resource.setResourceType(ResourceType.TOPIC.code());
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
