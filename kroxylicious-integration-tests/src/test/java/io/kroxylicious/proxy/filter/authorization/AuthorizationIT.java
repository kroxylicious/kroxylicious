/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.authorization;

import java.util.List;

import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.BaseIT;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;

public class AuthorizationIT extends BaseIT {

    // spin a cluster with Users:
    // Alice directly authorized for operation
    // Bob indirectly authorized for operation (by implication)
    // Eve not authorized for operation
    // do some prep (e.g. create a topic T, create a group G)
    // Make a request for T as each of Alice, Bob and Eve. Record the respose
    // Assert visible side effects
    // Tear down the cluster
    // spin a proxied cluster with proxy users authorised the same way
    // do the same prep (e.g. create a topic T, create a group G) (non proxied)
    // Make a proxied request for T as each of Alice, Bob and Eve
    // Assert that the responses are ==
    // Assert no visible side effects

    @Test
    void metadata(
                  short apiVersion,
                  boolean allowAutoCreation,
                  List<MetadataRequestData.MetadataRequestTopic> topics,
                  KafkaCluster kafkaCluster) {
        KroxyliciousTester t = null;

        var resp = t.simpleTestClient().getSync(new Request(ApiKeys.METADATA, apiVersion, "test",
                new MetadataRequestData()
                        .setAllowAutoTopicCreation(allowAutoCreation)
                        .setTopics(topics)
                        .setIncludeClusterAuthorizedOperations(true)
                        .setIncludeTopicAuthorizedOperations(true)));

        var r = (MetadataResponseData) resp.payload().message();
        r.errorCode();

    }
}
