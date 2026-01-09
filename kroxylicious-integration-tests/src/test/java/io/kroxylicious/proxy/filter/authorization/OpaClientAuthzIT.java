/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.testing.kafka.junit5ext.Name;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for OPA authorizer using the same test scenarios as ClientAuthzIT.
 * This test verifies that OPA authorizer behaves equivalently to ACL authorizer
 * by running the same test programs against both authorizers and comparing results.
 */
class OpaClientAuthzIT extends ClientAuthzIT {

    @Name("kafkaClusterWithAuthz")
    static Admin kafkaClusterWithAuthzAdmin;
    @Name("kafkaClusterNoAuthz")
    static Admin kafkaClusterNoAuthzAdmin;

    private Path opaPolicyFile;
    private Path opaDataFile;
    private List<org.apache.kafka.common.acl.AclBinding> aclBindings;

    @Override
    @BeforeAll
    void beforeAll() throws IOException {
        // Create temporary files for OPA policy and data
        opaPolicyFile = Files.createTempFile(getClass().getName(), ".wasm");
        opaDataFile = Files.createTempFile(getClass().getName(), ".json");

        // Copy OPA policy from test resources
        try (InputStream policyStream = getClass().getClassLoader()
                .getResourceAsStream("policies/client-authz/policy.wasm")) {
            if (policyStream == null) {
                throw new IllegalStateException("OPA policy file not found in test resources: policies/client-authz/policy.wasm");
            }
            Files.write(opaPolicyFile, policyStream.readAllBytes());
        }

        // Create OPA data file matching the ACL rules from ClientAuthzIT:
        // - alice can do ALL operations on topicA and topicB
        // - bob can CREATE topicA
        // - alice can do ALL operations on group1, group2, and txnIdP
        // Note: We use the topics map for all resource types (topics, groups, transactional IDs)
        // since the OPA policy checks by resourceName string
        String opaDataJson = """
                {
                    "topics": {
                        "%s": {
                            "owner": "alice",
                            "subscribers": []
                        },
                        "%s": {
                            "owner": "alice",
                            "subscribers": []
                        },
                        "%s": {
                            "owner": "alice",
                            "subscribers": []
                        },
                        "%s": {
                            "owner": "alice",
                            "subscribers": []
                        },
                        "%s": {
                            "owner": "alice",
                            "subscribers": []
                        }
                    },
                    "anyResourceUsers": ["bob"]
                }
                """.formatted(TOPIC_A, TOPIC_B, GROUP_1, GROUP_2, TXN_ID_P);

        Files.writeString(opaDataFile, opaDataJson);

        // Set up ACL bindings for the reference cluster (Kafka with ACLs)
        // This is used for comparison - the reference cluster uses Kafka ACLs
        aclBindings = List.of(
                new org.apache.kafka.common.acl.AclBinding(
                        new org.apache.kafka.common.resource.ResourcePattern(
                                org.apache.kafka.common.resource.ResourceType.TOPIC, TOPIC_A,
                                org.apache.kafka.common.resource.PatternType.LITERAL),
                        new org.apache.kafka.common.acl.AccessControlEntry("User:" + ALICE, "*",
                                org.apache.kafka.common.acl.AclOperation.ALL,
                                org.apache.kafka.common.acl.AclPermissionType.ALLOW)),
                new org.apache.kafka.common.acl.AclBinding(
                        new org.apache.kafka.common.resource.ResourcePattern(
                                org.apache.kafka.common.resource.ResourceType.TOPIC, TOPIC_B,
                                org.apache.kafka.common.resource.PatternType.LITERAL),
                        new org.apache.kafka.common.acl.AccessControlEntry("User:" + ALICE, "*",
                                org.apache.kafka.common.acl.AclOperation.ALL,
                                org.apache.kafka.common.acl.AclPermissionType.ALLOW)),
                new org.apache.kafka.common.acl.AclBinding(
                        new org.apache.kafka.common.resource.ResourcePattern(
                                org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID, TXN_ID_P,
                                org.apache.kafka.common.resource.PatternType.LITERAL),
                        new org.apache.kafka.common.acl.AccessControlEntry("User:" + ALICE, "*",
                                org.apache.kafka.common.acl.AclOperation.ALL,
                                org.apache.kafka.common.acl.AclPermissionType.ALLOW)),
                new org.apache.kafka.common.acl.AclBinding(
                        new org.apache.kafka.common.resource.ResourcePattern(
                                org.apache.kafka.common.resource.ResourceType.GROUP, GROUP_1,
                                org.apache.kafka.common.resource.PatternType.LITERAL),
                        new org.apache.kafka.common.acl.AccessControlEntry("User:" + ALICE, "*",
                                org.apache.kafka.common.acl.AclOperation.ALL,
                                org.apache.kafka.common.acl.AclPermissionType.ALLOW)),
                new org.apache.kafka.common.acl.AclBinding(
                        new org.apache.kafka.common.resource.ResourcePattern(
                                org.apache.kafka.common.resource.ResourceType.GROUP, GROUP_2,
                                org.apache.kafka.common.resource.PatternType.LITERAL),
                        new org.apache.kafka.common.acl.AccessControlEntry("User:" + ALICE, "*",
                                org.apache.kafka.common.acl.AclOperation.ALL,
                                org.apache.kafka.common.acl.AclPermissionType.ALLOW)),
                new org.apache.kafka.common.acl.AclBinding(
                        new org.apache.kafka.common.resource.ResourcePattern(
                                org.apache.kafka.common.resource.ResourceType.TOPIC, TOPIC_A,
                                org.apache.kafka.common.resource.PatternType.LITERAL),
                        new org.apache.kafka.common.acl.AccessControlEntry("User:" + BOB, "*",
                                org.apache.kafka.common.acl.AclOperation.CREATE,
                                org.apache.kafka.common.acl.AclPermissionType.ALLOW)));
    }

    @BeforeEach
    void prepClusters() {
        this.topicIdsInUnproxiedCluster = prepCluster(kafkaClusterWithAuthzAdmin, List.of(), aclBindings);
    }

    @AfterEach
    void tidyClusters() {
        deleteTopicsAndAcls(kafkaClusterWithAuthzAdmin, List.of(TOPIC_A), aclBindings);
        deleteTopicsAndAcls(kafkaClusterNoAuthzAdmin, List.of(TOPIC_A), List.of());
    }

    @AfterAll
    void afterAll() throws IOException {
        Files.deleteIfExists(opaPolicyFile);
        Files.deleteIfExists(opaDataFile);
    }

    @Override
    @ParameterizedTest
    @MethodSource
    void shouldEnforceAccessToTopics(Prog prog, List<Actor> actors) throws Exception {

        List<TracedOp> referenceResults;
        try (BaseClusterFixture cluster = new ReferenceCluster(kafkaClusterWithAuthz, this.topicIdsInUnproxiedCluster)) {
            referenceResults = traceExecution(actors, cluster, prog);
        }

        // Use OpaProxiedCluster instead of ProxiedCluster
        List<TracedOp> proxiedResults;
        try (var cluster = new OpaProxiedCluster(kafkaClusterNoAuthz, this.topicIdsInProxiedCluster,
                opaPolicyFile, opaDataFile)) {
            proxiedResults = traceExecution(actors, cluster, prog);
        }

        assertThat(proxiedResults).isEqualTo(referenceResults);
    }
}
