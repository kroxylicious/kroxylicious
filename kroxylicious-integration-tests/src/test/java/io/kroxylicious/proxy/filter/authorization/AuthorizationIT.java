/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.BaseIT;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(KafkaClusterExtension.class)
public class AuthorizationIT extends BaseIT {

    // 1. Spin a cluster with Users:
    //     * Alice directly authorized for operation
    //     * Bob indirectly authorized for operation (by implication)
    //     * Eve not authorized for operation
    // 2. Do some prep (e.g. create a topic T, create a group G)
    // 3. Make a request for T as each of Alice, Bob and Eve. Record the response
    // 4. Assert visible side effects
    // 5. Tear down the cluster
    // 6. Spin a proxied cluster with proxy users authorised the same way
    // 7. Do the same prep (e.g. create a topic T, create a group G) (non proxied)
    // 8. Make a proxied request for T as each of Alice, Bob and Eve
    // 9. Assert that the responses are ==
    // 10. Assert no visible side effects

    static List<Arguments> metadata() {
        List<Arguments> result = new ArrayList<>();
        for (var apiVersion : ApiKeys.METADATA.allVersions()) {
            for (boolean allowAutoCreation : new boolean[]{true, false}) {
                if (!allowAutoCreation && apiVersion < 4) {
                    continue;
                }
                for (List<MetadataRequestData.MetadataRequestTopic> topics : new List[]{null, List.of()}) {
                    if (topics == null && apiVersion < 1) {
                        continue;
                    }
                    for (boolean includeClusterAuthz : new boolean[]{true, false}) {
                        if (apiVersion < 8 && includeClusterAuthz) {
                            continue;
                        }
                        if (apiVersion >= 11 && includeClusterAuthz) {
                            continue;
                        }
                        for (boolean includeTopicAuthz : new boolean[]{true, false}) {
                            if (apiVersion < 8 && includeTopicAuthz) {
                                continue;
                            }
                            result.addAll(List.of(
                                    Arguments.of(apiVersion, allowAutoCreation, topics, includeClusterAuthz, includeTopicAuthz)));
                        }
                    }
                }}
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource
    void metadata(
                  short apiVersion,
                  boolean allowAutoCreation,
                  List<MetadataRequestData.MetadataRequestTopic> topics,
                  boolean includeClusterAuthz,
                  boolean includeTopicsAuthz,
                  @SaslMechanism(principals = {
                          @SaslMechanism.Principal(user="alice", password="Alice"),
                          @SaslMechanism.Principal(user="bob", password="Bob"),
                          @SaslMechanism.Principal(user="eve", password="Eve")
                  })
                  KafkaCluster kafkaCluster,
                  KafkaCluster kafkaCluster2) {

        Uuid topicId;
        try (var admin = AdminClient.create(kafkaCluster.getKafkaClientConfiguration())) {
            topicId = admin.createTopics(List.of(new NewTopic("topic", 1, (short) 1)))
                    .topicId("topic")
                    .toCompletionStage().toCompletableFuture().join();
//            admin.createAcls(List.of(new AclBinding(new ResourcePattern(ResourceType.TOPIC, "topic", PatternType.LITERAL),
//                    new AccessControlEntry("alice", "*", AclOperation.CREATE, AclPermissionType.ALLOW)))).all()
//                    .toCompletionStage().toCompletableFuture().join();
        }
        Map<String, MetadataResponseData> responsesByUser = new HashMap<>();

        for (var entry : Map.of(
                "alice", "Alice",
                "bob", "Bob",
                "eve", "Eve").entrySet()) {
            try (KafkaClient client = client(kafkaCluster)) {
                authenticate(client, entry.getKey(), entry.getValue());

                var resp = client.getSync(new Request(ApiKeys.METADATA, apiVersion, "test",
                        new MetadataRequestData()
                                .setAllowAutoTopicCreation(allowAutoCreation)
                                .setTopics(topics)
                                .setIncludeClusterAuthorizedOperations(includeClusterAuthz)
                                .setIncludeTopicAuthorizedOperations(includeTopicsAuthz)));

                var r = (MetadataResponseData) resp.payload().message();
                assertThat(Errors.forCode(r.errorCode())).isEqualTo(Errors.NONE);
                responsesByUser.put(entry.getKey(), r);
            }
        }
    }

    private static void authenticate(KafkaClient client, String username, String password) {
        var h = (SaslHandshakeResponseData) client.getSync(new Request(ApiKeys.SASL_HANDSHAKE, ApiKeys.SASL_HANDSHAKE.latestVersion(), "test",
                new SaslHandshakeRequestData()
                        .setMechanism("PLAIN")
                        )).payload().message();
        assertThat(Errors.forCode(h.errorCode())).isEqualTo(Errors.NONE);
        var a = (SaslAuthenticateResponseData) client.getSync(new Request(ApiKeys.SASL_AUTHENTICATE, ApiKeys.SASL_AUTHENTICATE.latestVersion(), "test",
                new SaslAuthenticateRequestData()
                        .setAuthBytes((username + "\0" + username + "\0" + password).getBytes(StandardCharsets.UTF_8))
        )).payload().message();
        assertThat(Errors.forCode(a.errorCode())).isEqualTo(Errors.NONE);
    }

    @NonNull
    private static KafkaClient client(KafkaCluster kafkaCluster) {
        String bootstrapServers = kafkaCluster.getBootstrapServers();
        String[] hostPort = bootstrapServers.split(",")[0].split(":");
        KafkaClient client = new KafkaClient(hostPort[0], Integer.parseInt(hostPort[1]));
        return client;
    }
}
